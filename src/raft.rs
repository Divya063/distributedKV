use raft-rs::{Node, Raft};
use raft-rs::storage::MemStorage;

fn main() {
    let mut raft = Raft::new(Node::new(1), MemStorage::new());
    raft.become_candidate();
    raft.become_leader();
    raft.become_follower();
}

