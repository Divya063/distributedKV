use raft::prelude::{RawNode,Config};
use raft::storage::MemStorage;
use raft::eraftpb::Snapshot;
use::std::collections::HashMap;

pub struct Node {
    pub node: RawNode<MemStorage>,
    pub key_value_store: HashMap<String, String>,
}

impl Node {
    pub fn create_leader_node()-> Self{
        let config = Config {
            id: 1,
            ..Default::default()
        };
        let logger = raft::default_logger();
        let storage = MemStorage::new();
        let mut s = Snapshot::default();
        s.mut_metadata().index = 1;
        s.mut_metadata().term = 1;
        s.mut_metadata().mut_conf_state().voters = vec![1];

        storage.wl().apply_snapshot(s).unwrap();
        let mut node = RawNode::new(&config, storage, &logger).unwrap();
        
        // mandatory to become a candidate first: invalid transition [follower -> leader]
        node.raft.become_candidate();
        node.raft.become_leader();
        Node {
            node: node,
            key_value_store: HashMap::new(),
        }
    }

    pub fn create_follower_node()-> Self{
        let config = Config {
            id: 2,
            ..Default::default()
        };
        let logger = raft::default_logger();
        let storage = MemStorage::new();
        let mut node = RawNode::new(&config, storage, &logger).unwrap();
        node.raft.become_follower(1, 1);
        Node {
            node: node,
            key_value_store: HashMap::new(),
        }
    }
}

