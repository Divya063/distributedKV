use raft::prelude::{RawNode,Config};
use raft::storage::MemStorage;
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
        let mut node = RawNode::new(&config, storage, &logger).unwrap();
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
        Node {
            node: node,
            key_value_store: HashMap::new(),
        }
    }
}

