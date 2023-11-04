mod raft_impl;
use slog::{Drain, Logger};
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use raft::eraftpb::ConfState;
use raft::prelude::*;
use raft::storage::MemStorage;
use slog_term;
use slog_async;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use raft_impl::*;

use slog::{info, o};

type ProposeCallback = Box<dyn Fn() + Send>;

enum Msg {
    Propose {
        id: u8,
        data: Vec<u8>,
        cb: ProposeCallback,
    },
    #[allow(dead_code)]
    Raft(Message),
}

fn main() {

    let mut leader_node = Node::create_leader_node();
    let node = Arc::new(Mutex::new(leader_node));
    let sender = node.lock().unwrap().sender.clone();

    // to avoid blocking the main thread, we run the Raft in another thread.
    let handler2 = thread::spawn(move || {raft_impl::start(node)});
    let key = "foo".to_owned();
    let value = "bar".to_owned();
    put(sender, key, value);
    


    handler2.join().unwrap(); 
}

fn put(sender: Sender<raft_impl::Msg> , key: String, value: String){
    raft_impl::send_propose(sender, key, value);
}