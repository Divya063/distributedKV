mod raft_impl;
use slog::{Drain, Logger};
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use raft_impl::*;


fn main() {
    let mut mailbox = HashMap::new();
    let (sender_leader, receiver_leader) = mpsc::channel();
    let (sender_follower, receiver_follower) = mpsc::channel();
    mailbox.insert(1, sender_leader.clone());
    mailbox.insert(2, sender_follower);
    let mut leader_node = Node::create_leader_node(mailbox.clone(), receiver_leader);
    let logger = leader_node.logger.clone();
    let node = Arc::new(Mutex::new(leader_node));

    // to avoid blocking the main thread, we run the Raft in another thread.
    let handler = thread::spawn(move || {raft_impl::start(node)});
    let key = "foo".to_owned();
    let value = "bar".to_owned();
    
    // create the follower node
    let mut follower_node = Node::create_follower_node(mailbox.clone(), receiver_follower, logger.clone());
    let follower_node = Arc::new(Mutex::new(follower_node));
    let follower_handler = thread::spawn(move || {raft_impl::start(follower_node)});
    raft_impl::add_followers(2, sender_leader.clone());

    put(logger.clone(), sender_leader.clone(), key, value);

    handler.join().unwrap(); 
    follower_handler.join().unwrap();
}

fn put(logger: Logger, sender: Sender<raft_impl::Msg> , key: String, value: String){
    raft_impl::send_propose(logger, sender, key, value);
}