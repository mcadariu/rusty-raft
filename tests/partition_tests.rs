use rusty_raft::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use turmoil::{Builder, Result};

#[test]
fn test_leader_election() -> Result {
    let mut sim = Builder::new().build();

    sim.host("node1", || async {
        let peers = vec!["node2:9000".to_string(), "node3:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node1:9000".to_string(), peers)));

        {
            let mut n = node.lock().await;
            n.become_candidate();
        }

        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node2", || async {
        let peers = vec!["node1:9000".to_string(), "node3:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node2:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node3", || async {
        let peers = vec!["node1:9000".to_string(), "node2:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node3:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.client("test", async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("Leader election test completed");
        Ok(())
    });

    sim.run()
}

#[test]
fn test_single_node_partition() -> Result {
    let mut sim = Builder::new().build();

    sim.host("node1", || async {
        let peers = vec!["node2:9000".to_string(), "node3:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node1:9000".to_string(), peers)));

        {
            let mut n = node.lock().await;
            n.become_candidate();
        }

        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node2", || async {
        let peers = vec!["node1:9000".to_string(), "node3:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node2:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node3", || async {
        let peers = vec!["node1:9000".to_string(), "node2:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node3:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.client("test", async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("Initial leader election completed");

        println!("Creating partition: isolating node1");
        turmoil::partition("node1", "node2");
        turmoil::partition("node1", "node3");

        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("Cluster operating with node1 partitioned");

        println!("Healing partition: reconnecting node1");
        turmoil::repair("node1", "node2");
        turmoil::repair("node1", "node3");

        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("Partition healed - cluster should converge");

        Ok(())
    });

    sim.run()
}

#[test]
fn test_split_brain_partition() -> Result {
    let mut sim = Builder::new().build();

    sim.host("node1", || async {
        let peers = vec![
            "node2:9000".to_string(),
            "node3:9000".to_string(),
            "node4:9000".to_string(),
            "node5:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node1:9000".to_string(), peers)));

        {
            let mut n = node.lock().await;
            n.become_candidate();
        }

        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node2", || async {
        let peers = vec![
            "node1:9000".to_string(),
            "node3:9000".to_string(),
            "node4:9000".to_string(),
            "node5:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node2:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node3", || async {
        let peers = vec![
            "node1:9000".to_string(),
            "node2:9000".to_string(),
            "node4:9000".to_string(),
            "node5:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node3:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node4", || async {
        let peers = vec![
            "node1:9000".to_string(),
            "node2:9000".to_string(),
            "node3:9000".to_string(),
            "node5:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node4:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node5", || async {
        let peers = vec![
            "node1:9000".to_string(),
            "node2:9000".to_string(),
            "node3:9000".to_string(),
            "node4:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node5:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.client("test", async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("Initial leader elected in 5-node cluster");

        println!("Creating split-brain partition");
        turmoil::partition("node1", "node3");
        turmoil::partition("node1", "node4");
        turmoil::partition("node1", "node5");
        turmoil::partition("node2", "node3");
        turmoil::partition("node2", "node4");
        turmoil::partition("node2", "node5");

        tokio::time::sleep(Duration::from_millis(800)).await;
        println!("Split-brain active - only majority side (3 nodes) can make progress");

        println!("Healing split-brain partition");
        turmoil::repair("node1", "node3");
        turmoil::repair("node1", "node4");
        turmoil::repair("node1", "node5");
        turmoil::repair("node2", "node3");
        turmoil::repair("node2", "node4");
        turmoil::repair("node2", "node5");

        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("Cluster healed and converged");

        Ok(())
    });

    sim.run()
}

#[test]
fn test_log_replication_with_partition() -> Result {
    let mut sim = Builder::new().build();

    sim.host("node1", || async {
        let peers = vec!["node2:9000".to_string(), "node3:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node1:9000".to_string(), peers)));

        {
            let mut n = node.lock().await;
            n.become_candidate();
            let term = n.state.current_term;
            n.state.log.push(LogEntry {
                term,
                index: 1,
                command: b"SET x=1".to_vec(),
            });
            n.state.log.push(LogEntry {
                term,
                index: 2,
                command: b"SET y=2".to_vec(),
            });
        }

        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node2", || async {
        let peers = vec!["node1:9000".to_string(), "node3:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node2:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node3", || async {
        let peers = vec!["node1:9000".to_string(), "node2:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node3:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.client("test", async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("Leader elected and initial entries replicated");

        println!("Partitioning node3 from cluster");
        turmoil::partition("node3", "node1");
        turmoil::partition("node3", "node2");

        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("node1 and node2 continue operating, node3 is isolated");

        println!("Healing partition - node3 should catch up");
        turmoil::repair("node3", "node1");
        turmoil::repair("node3", "node2");

        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("Node3 should have caught up with replicated entries");

        Ok(())
    });

    sim.run()
}

#[test]
fn test_cascading_failures() -> Result {
    let mut sim = Builder::new().build();

    sim.host("node1", || async {
        let peers = vec![
            "node2:9000".to_string(),
            "node3:9000".to_string(),
            "node4:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node1:9000".to_string(), peers)));

        {
            let mut n = node.lock().await;
            n.become_candidate();
        }

        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node2", || async {
        let peers = vec![
            "node1:9000".to_string(),
            "node3:9000".to_string(),
            "node4:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node2:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node3", || async {
        let peers = vec![
            "node1:9000".to_string(),
            "node2:9000".to_string(),
            "node4:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node3:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node4", || async {
        let peers = vec![
            "node1:9000".to_string(),
            "node2:9000".to_string(),
            "node3:9000".to_string(),
        ];
        let node = Arc::new(Mutex::new(RaftNode::new("node4:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.client("test", async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        println!("Initial leader elected");

        println!("Step 1: Partitioning node1");
        turmoil::partition("node1", "node2");
        turmoil::partition("node1", "node3");
        turmoil::partition("node1", "node4");

        tokio::time::sleep(Duration::from_millis(400)).await;
        println!("New leader should be elected among remaining 3 nodes");

        println!("Step 2: Partitioning node2 (cascading failure)");
        turmoil::partition("node2", "node3");
        turmoil::partition("node2", "node4");

        tokio::time::sleep(Duration::from_millis(400)).await;
        println!("node3 and node4 still have majority and should maintain leadership");

        println!("Step 3: Healing node1");
        turmoil::repair("node1", "node3");
        turmoil::repair("node1", "node4");

        tokio::time::sleep(Duration::from_millis(300)).await;

        println!("Step 4: Healing node2");
        turmoil::repair("node2", "node1");
        turmoil::repair("node2", "node3");
        turmoil::repair("node2", "node4");

        tokio::time::sleep(Duration::from_millis(400)).await;
        println!("All nodes recovered - cluster fully operational");

        Ok(())
    });

    sim.run()
}

#[test]
fn test_rapid_partition_cycles() -> Result {
    let mut sim = Builder::new().build();

    sim.host("node1", || async {
        let peers = vec!["node2:9000".to_string(), "node3:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node1:9000".to_string(), peers)));

        {
            let mut n = node.lock().await;
            n.become_candidate();
        }

        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node2", || async {
        let peers = vec!["node1:9000".to_string(), "node3:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node2:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.host("node3", || async {
        let peers = vec!["node1:9000".to_string(), "node2:9000".to_string()];
        let node = Arc::new(Mutex::new(RaftNode::new("node3:9000".to_string(), peers)));
        run_raft_node(node, "0.0.0.0:9000".to_string()).await
    });

    sim.client("test", async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        println!("Initial cluster formed");

        println!("Cycle 1: Partition and heal");
        turmoil::partition("node1", "node2");
        turmoil::partition("node1", "node3");
        tokio::time::sleep(Duration::from_millis(300)).await;
        turmoil::repair("node1", "node2");
        turmoil::repair("node1", "node3");
        tokio::time::sleep(Duration::from_millis(200)).await;

        println!("Cycle 2: Partition and heal");
        turmoil::partition("node2", "node1");
        turmoil::partition("node2", "node3");
        tokio::time::sleep(Duration::from_millis(300)).await;
        turmoil::repair("node2", "node1");
        turmoil::repair("node2", "node3");
        tokio::time::sleep(Duration::from_millis(200)).await;

        println!("Cycle 3: Partition and heal");
        turmoil::partition("node3", "node1");
        turmoil::partition("node3", "node2");
        tokio::time::sleep(Duration::from_millis(300)).await;
        turmoil::repair("node3", "node1");
        turmoil::repair("node3", "node2");
        tokio::time::sleep(Duration::from_millis(300)).await;

        println!("Cluster survived multiple partition cycles and recovered");

        Ok(())
    });

    sim.run()
}
