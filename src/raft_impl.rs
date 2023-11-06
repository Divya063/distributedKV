
#![allow(clippy::field_reassign_with_default)]

use raft::prelude::{RawNode,Config};
use raft::storage::MemStorage;
use raft::eraftpb::{Snapshot, EntryType, Message};
use::std::collections::HashMap;
use slog::{Drain, o, info};

use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use raft::{prelude::*, StateRole};
use slog_term;
use slog_async;
use std::sync::mpsc::{self, Receiver};
use std::sync::mpsc::RecvTimeoutError;
use slog::Logger;
use std::sync::{Arc, Mutex}; 
use protobuf::Message as PbMessage;



type ProposeCallback = Box<dyn Fn() + Send>;

pub enum Msg {
    Propose {
        // id: u8,
        data: Vec<u8>,
        // cb: ProposeCallback,
    },
    #[allow(dead_code)]
    Raft(Message),
    ConfChange(ConfChange),
}


pub struct Node {
    pub node: RawNode<MemStorage>,
    pub logger: slog::Logger,
    pub key_value_store: HashMap<String, String>,
    // mailbox to communicate with other nodes
    pub mailbox: HashMap<u64, mpsc::Sender<Msg>>,
    pub receiver: mpsc::Receiver<Msg>,
}

impl Node {
    pub fn create_leader_node(mailbox: HashMap<u64, mpsc::Sender<Msg>>, receiver: Receiver<Msg>)-> Self{
        let config = Config {
            id: 1,
            heartbeat_tick: 3,
            ..Default::default()
        };
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain)
            .chan_size(4096)
            .overflow_strategy(slog_async::OverflowStrategy::Block)
            .build()
            .fuse();

        let logger = slog::Logger::root(drain, o!());
        let storage = MemStorage::new();
        let mut s = Snapshot::default();
        // state to start with
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
            logger: logger,
            receiver: receiver,
            mailbox: mailbox,
            key_value_store: HashMap::new(),
         }

    }


    pub fn create_follower_node(mailbox: HashMap<u64, mpsc::Sender<Msg>>, receiver: Receiver<Msg>, logger: slog::Logger)-> Self{
        let config = Config {
            id: 2,
            ..Default::default()
        };
        let storage = MemStorage::new();
        let mut node = RawNode::new(&config, storage, &logger).unwrap();
        Node {
            node: node,
            logger: logger,
            receiver: receiver,
            mailbox: mailbox,
            key_value_store: HashMap::new(),
        }
    }

    pub fn get_value(&self, key: &str) -> Option<String> {
        self.key_value_store.get(key).cloned()
    }
   
}

pub fn start(node: Arc<Mutex<Node>>) {
    let mut cbs = HashMap::new();
    let cloned_node = Arc::clone(&node);
    loop {
         // Loop forever to drive the Raft.
        let mut node = cloned_node.lock().unwrap();
        let mut t = Instant::now();
        let mut timeout = Duration::from_millis(500);
            match node.receiver.recv_timeout(timeout) {
                Ok(Msg::Propose { data }) => {
                    if node.node.raft.state == StateRole::Leader {
                        // cbs.insert(id, cb);
                        node.node.propose(vec![], data).unwrap();
                    } else {
                        continue;
                    }
                }
                Ok(Msg::ConfChange(cc)) => {
                    let cs = node.node.apply_conf_change(&cc).unwrap();
                    let store = node.node.raft.raft_log.store.clone();
                    // set the conf state to restore the snapshot, this will avoid the error - attempted to restore snapshot but it is not in the ConfState
                    store.wl().set_conf_state(cs);
                }
                Ok(Msg::Raft(m)) => node.node.step(m).unwrap(),
                Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => return,
            }
    
            let d = t.elapsed();
            t = Instant::now();
            if d >= timeout {
                timeout = Duration::from_millis(100);
                // We drive Raft every 100ms.
                node.node.tick();
            } else {
                timeout -= d;
            }
        on_ready(&mut node, &mut cbs);
        }
 
}

fn on_ready(node: &mut Node,cbs: &mut HashMap<u8, ProposeCallback>) {
    let logger = node.logger.clone();
    if !node.node.has_ready() {
        return;
    }
    let store = node.node.raft.raft_log.store.clone();

    // Get the `Ready` with `RawNode::ready` interface.
    let mut ready = node.node.ready();

    let handle_messages = |node: &mut Node, msgs: Vec<Message>| {
        for msg in msgs {
            let to = msg.to;
            node.mailbox.get(&to).unwrap().send(Msg::Raft(msg)).unwrap();

        }
    };

    if !ready.messages().is_empty() {
        // Send out the messages come from the node.
        handle_messages(node, ready.take_messages());
    }

    if !ready.snapshot().is_empty() {
        // This is a snapshot, we need to apply the snapshot at first.
        store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
    }

    let mut _last_apply_index = 0;
    let mut handle_committed_entries = |node: &mut Node, committed_entries: Vec<Entry>| {
        info!(logger, "handle_committed_entries"; "committed_entries" => format!("{:?}", committed_entries));
        for entry in committed_entries {

            if entry.data.is_empty() {
                // Empty entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }
            if let EntryType::EntryConfChange = entry.get_entry_type() {
                // For conf change messages, make them effective.
                let mut cc = ConfChange::default();
                // TODO
                cc.merge_from_bytes(&entry.data);
                let cs = node.node.apply_conf_change(&cc).unwrap();
                store.wl().set_conf_state(cs);
            }

            if entry.get_entry_type() == EntryType::EntryNormal {
                print!("entry data: {:?}", entry.data);
                let data = std::str::from_utf8(&entry.data).unwrap();
                let temp_data: Vec<&str> = data.split(" ").collect();
                let key = temp_data[0].to_owned();
                let value = temp_data[1].to_owned();
                info!(logger, "key {} value {}", key, value);
                node.key_value_store.insert(key, value);
                info!(logger, "handle_committed_entries {} node id {}", data, node.node.raft.id);
                // if let Some(cb) = cbs.remove(entry.data.first().unwrap()) {
                //     cb();
                // }
            }
        }
    };
    handle_committed_entries(node, ready.take_committed_entries());

    if !ready.entries().is_empty() {
        // Append entries to the Raft log.
        store.wl().append(ready.entries()).unwrap();
    }


    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        store.wl().set_hardstate(hs.clone());
    }

    if !ready.persisted_messages().is_empty() {
        // Send out the persisted messages come from the node.
        handle_messages(node, ready.take_persisted_messages());
    }

    // Advance the Raft.
    let mut light_rd = node.node.advance(ready);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        store.wl().mut_hard_state().set_commit(commit);
    }
    // Send out the messages.
    handle_messages(node, light_rd.take_messages());
    // Apply all committed entries.
    handle_committed_entries(node, light_rd.take_committed_entries());
    // Advance the apply index.
    node.node.advance_apply();
}

pub fn send_propose(logger: slog::Logger , sender: mpsc::Sender<Msg>, key: String, value: String) {
    thread::spawn( move || {   
        // let (s1, r1) = mpsc::channel::<u8>();
        let data = format!("{} {}", key, value).into_bytes();

        // Send a command to the Raft, wait for the Raft to apply it
        // and get the result.
        info!(logger, "hello");
        sender
            .send(Msg::Propose {
                data: data,
                })
            .unwrap();
        println!("send_propose");



    });
    
}

fn conf_change(t: ConfChangeType, node_id: u64) -> ConfChange {
    let mut cc = ConfChange::default();
    cc.set_change_type(t);
    cc.node_id = node_id;
    cc
}

pub fn add_followers(node_id: u64, sender: mpsc::Sender<Msg>) {
    let cc = conf_change(ConfChangeType::AddNode, node_id);
    sender.send(Msg::ConfChange((&cc).clone())).unwrap();

}

pub fn remove_followers(node_id: u64, sender: mpsc::Sender<Msg>) {
    let cc = conf_change(ConfChangeType::RemoveNode, node_id);
    sender.send(Msg::ConfChange((&cc).clone())).unwrap();
}