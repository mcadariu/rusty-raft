use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, info};

pub type NodeId = String;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
}

pub struct RaftState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,

    pub commit_index: LogIndex,
    pub last_applied: LogIndex,

    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }
}

pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub role: Role,
    pub state: RaftState,
    pub leader_id: Option<NodeId>,
    pub votes_received: usize,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        RaftNode {
            id,
            peers,
            role: Role::Follower,
            state: RaftState::default(),
            leader_id: None,
            votes_received: 0,
        }
    }

    fn last_log_index(&self) -> LogIndex {
        self.state.log.len() as LogIndex
    }

    fn last_log_term(&self) -> Term {
        self.state.log.last().map(|e| e.term).unwrap_or(0)
    }

    fn majority(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }

    fn check_term(&mut self, term: Term) -> bool {
        if term > self.state.current_term {
            self.become_follower(term);
            true
        } else {
            false
        }
    }

    pub fn become_follower(&mut self, term: Term) {
        info!("[{}] Becoming follower for term {}", self.id, term);
        self.role = Role::Follower;
        self.state.current_term = term;
        self.state.voted_for = None;
        self.votes_received = 0;
        self.leader_id = None;
    }

    pub fn become_candidate(&mut self) {
        self.state.current_term += 1;
        self.role = Role::Candidate;
        self.state.voted_for = Some(self.id.clone());
        self.votes_received = 1;
        self.leader_id = None;
        info!("[{}] Becoming candidate for term {}", self.id, self.state.current_term);
    }

    pub fn become_leader(&mut self) {
        info!("[{}] Becoming leader for term {}", self.id, self.state.current_term);
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());

        let next_index = self.last_log_index() + 1;
        for peer in &self.peers {
            self.state.next_index.insert(peer.clone(), next_index);
            self.state.match_index.insert(peer.clone(), 0);
        }
    }

    pub fn handle_request_vote(&mut self, req: RequestVoteRequest) -> RequestVoteResponse {
        debug!("[{}] Received RequestVote from {} for term {}",
            self.id, req.candidate_id, req.term);

        self.check_term(req.term);

        let log_ok = req.last_log_term > self.last_log_term() ||
            (req.last_log_term == self.last_log_term() && req.last_log_index >= self.last_log_index());

        let vote_granted = req.term == self.state.current_term &&
            log_ok &&
            (self.state.voted_for.is_none() || self.state.voted_for.as_ref() == Some(&req.candidate_id));

        if vote_granted {
            self.state.voted_for = Some(req.candidate_id.clone());
            info!("[{}] Granted vote to {} for term {}", self.id, req.candidate_id, req.term);
        }

        RequestVoteResponse {
            term: self.state.current_term,
            vote_granted,
        }
    }

    pub fn handle_append_entries(&mut self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        debug!("[{}] Received AppendEntries from {} for term {} with {} entries",
            self.id, req.leader_id, req.term, req.entries.len());

        if req.term > self.state.current_term {
            self.become_follower(req.term);
        }

        if req.term < self.state.current_term {
            return AppendEntriesResponse {
                term: self.state.current_term,
                success: false,
                match_index: 0,
            };
        }

        if self.role == Role::Candidate {
            self.become_follower(req.term);
        }

        self.leader_id = Some(req.leader_id.clone());

        if req.prev_log_index > 0 {
            if req.prev_log_index > self.state.log.len() as LogIndex {
                return AppendEntriesResponse {
                    term: self.state.current_term,
                    success: false,
                    match_index: self.state.log.len() as LogIndex,
                };
            }

            let prev_entry = &self.state.log[req.prev_log_index as usize - 1];
            if prev_entry.term != req.prev_log_term {
                self.state.log.truncate(req.prev_log_index as usize - 1);
                return AppendEntriesResponse {
                    term: self.state.current_term,
                    success: false,
                    match_index: self.state.log.len() as LogIndex,
                };
            }
        }

        for entry in req.entries {
            if entry.index <= self.state.log.len() as LogIndex {
                if self.state.log[entry.index as usize - 1].term != entry.term {
                    self.state.log.truncate(entry.index as usize - 1);
                    self.state.log.push(entry);
                }
            } else {
                self.state.log.push(entry);
            }
        }

        if req.leader_commit > self.state.commit_index {
            self.state.commit_index = req.leader_commit.min(self.state.log.len() as LogIndex);
            debug!("[{}] Updated commit_index to {}", self.id, self.state.commit_index);
        }

        AppendEntriesResponse {
            term: self.state.current_term,
            success: true,
            match_index: self.state.log.len() as LogIndex,
        }
    }

    pub fn handle_vote_response(&mut self, resp: RequestVoteResponse) {
        if resp.term > self.state.current_term {
            self.become_follower(resp.term);
            return;
        }

        if self.role == Role::Candidate && resp.term == self.state.current_term && resp.vote_granted {
            self.votes_received += 1;
            let majority = (self.peers.len() + 1) / 2 + 1;

            debug!("[{}] Received vote, total votes: {}/{}",
                self.id, self.votes_received, majority);

            if self.votes_received >= majority {
                self.become_leader();
            }
        }
    }

    pub fn handle_append_entries_response(&mut self, peer: NodeId, resp: AppendEntriesResponse) {
        if resp.term > self.state.current_term {
            self.become_follower(resp.term);
            return;
        }

        if self.role != Role::Leader || resp.term != self.state.current_term {
            return;
        }

        if resp.success {
            self.state.match_index.insert(peer.clone(), resp.match_index);
            self.state.next_index.insert(peer.clone(), resp.match_index + 1);

            let mut match_indices: Vec<LogIndex> =
                self.state.match_index.values().copied().collect();
            match_indices.push(self.state.log.len() as LogIndex);
            match_indices.sort_unstable();

            let majority_index = match_indices[match_indices.len() / 2];

            if majority_index > self.state.commit_index {
                if let Some(entry) = self.state.log.get(majority_index as usize - 1) {
                    if entry.term == self.state.current_term {
                        self.state.commit_index = majority_index;
                        info!("[{}] Leader updated commit_index to {}",
                            self.id, self.state.commit_index);
                    }
                }
            }
        } else {
            let next_idx = self.state.next_index.get(&peer).copied().unwrap_or(1);
            if next_idx > 1 {
                self.state.next_index.insert(peer, next_idx - 1);
            }
        }
    }
}

pub async fn send_message(addr: String, msg: RaftMessage) -> Option<RaftMessage> {
    match timeout(Duration::from_millis(100), TcpStream::connect(&addr)).await {
        Ok(Ok(mut stream)) => {
            let encoded = bincode::serialize(&msg).ok()?;
            if stream.write_all(&encoded).await.is_err() {
                return None;
            }

            let mut buf = vec![0u8; 8192];
            match timeout(Duration::from_millis(100), stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    bincode::deserialize(&buf[..n]).ok()
                }
                _ => None,
            }
        }
        _ => None,
    }
}

pub async fn run_raft_node(
    node: Arc<Mutex<RaftNode>>,
    listen_addr: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let node_id = {
        let n = node.lock().await;
        n.id.clone()
    };

    let node_clone = node.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind(&listen_addr).await.unwrap();
        info!("[{}] Listening on {}", node_id, listen_addr);

        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let node = node_clone.clone();

            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                if let Ok(n) = socket.read(&mut buf).await {
                    if n == 0 {
                        return;
                    }

                    if let Ok(msg) = bincode::deserialize::<RaftMessage>(&buf[..n]) {
                        let mut node = node.lock().await;
                        let response = match msg {
                            RaftMessage::RequestVote(req) => {
                                RaftMessage::RequestVoteResponse(node.handle_request_vote(req))
                            }
                            RaftMessage::AppendEntries(req) => {
                                RaftMessage::AppendEntriesResponse(node.handle_append_entries(req))
                            }
                            _ => return,
                        };

                        if let Ok(encoded) = bincode::serialize(&response) {
                            let _ = socket.write_all(&encoded).await;
                        }
                    }
                }
            });
        }
    });

    loop {
        sleep(Duration::from_millis(50)).await;

        let role = {
            let n = node.lock().await;
            n.role.clone()
        };

        match role {
            Role::Follower => {
            }
            Role::Candidate => {
                run_election(node.clone()).await;
            }
            Role::Leader => {
                send_heartbeats(node.clone()).await;
            }
        }
    }
}

async fn run_election(node: Arc<Mutex<RaftNode>>) {
    let (peers, term, last_log_index, last_log_term, node_id) = {
        let n = node.lock().await;
        let last_log_index = n.state.log.len() as LogIndex;
        let last_log_term = n.state.log.last().map(|e| e.term).unwrap_or(0);
        (n.peers.clone(), n.state.current_term, last_log_index, last_log_term, n.id.clone())
    };

    for peer in peers {
        let node = node.clone();
        let peer_addr = peer.clone();
        let request = RequestVoteRequest {
            term,
            candidate_id: node_id.clone(),
            last_log_index,
            last_log_term,
        };

        tokio::spawn(async move {
            if let Some(RaftMessage::RequestVoteResponse(resp)) =
                send_message(peer_addr, RaftMessage::RequestVote(request)).await {
                let mut n = node.lock().await;
                n.handle_vote_response(resp);
            }
        });
    }
}

async fn send_heartbeats(node: Arc<Mutex<RaftNode>>) {
    let (peers, term, leader_id, commit_index) = {
        let n = node.lock().await;
        (n.peers.clone(), n.state.current_term, n.id.clone(), n.state.commit_index)
    };

    for peer in peers {
        let node = node.clone();
        let peer_id = peer.clone();

        let (prev_log_index, prev_log_term, entries) = {
            let n = node.lock().await;
            let next_idx = n.state.next_index.get(&peer_id).copied().unwrap_or(1);
            let prev_log_index = if next_idx > 1 { next_idx - 1 } else { 0 };
            let prev_log_term = if prev_log_index > 0 {
                n.state.log.get(prev_log_index as usize - 1).map(|e| e.term).unwrap_or(0)
            } else {
                0
            };
            let entries = n.state.log.iter()
                .skip(next_idx.saturating_sub(1) as usize)
                .cloned()
                .collect();
            (prev_log_index, prev_log_term, entries)
        };

        let request = AppendEntriesRequest {
            term,
            leader_id: leader_id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: commit_index,
        };

        tokio::spawn(async move {
            if let Some(RaftMessage::AppendEntriesResponse(resp)) =
                send_message(peer_id.clone(), RaftMessage::AppendEntries(request)).await {
                let mut n = node.lock().await;
                n.handle_append_entries_response(peer_id, resp);
            }
        });
    }
}
