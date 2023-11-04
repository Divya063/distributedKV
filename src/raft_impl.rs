use raft::prelude::{RawNode,Config};
use raft::storage::MemStorage;
use raft::eraftpb::Snapshot;
use::std::collections::HashMap;
use slog::{Drain, o, info};

use std::thread;
use std::time::{Duration, Instant};

use raft::prelude::*;
use slog_term;
use slog_async;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use slog::Logger;
use std::sync::{Arc, Mutex}; 


type ProposeCallback = Box<dyn Fn() + Send>;

pub enum Msg {
    Propose {
        id: u8,
        data: Vec<u8>,
        cb: ProposeCallback,
    },
    #[allow(dead_code)]
    Raft(Message),
}


pub struct Node {
    pub node: RawNode<MemStorage>,
    pub logger: slog::Logger,
    pub key_value_store: HashMap<String, String>,
    pub sender: mpsc::Sender<Msg>,
    pub receiver: mpsc::Receiver<Msg>,
}

impl Node {
    pub fn create_leader_node()-> Self{
        let config = Config {
            id: 1,
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
        let (sender, receiver) = mpsc::channel();
        let mut node = RawNode::new(&config, storage, &logger).unwrap();
        
        // mandatory to become a candidate first: invalid transition [follower -> leader]
        node.raft.become_candidate();
        node.raft.become_leader();
         Node {
            node: node,
            logger: logger,
            sender: sender,
            receiver: receiver,
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
        let (sender, receiver) = mpsc::channel();
        Node {
            node: node,
            logger: logger,
            sender: sender,
            receiver: receiver,
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
        let mut timeout = Duration::from_millis(100);
            match node.receiver.recv_timeout(timeout) {
                Ok(Msg::Propose { id, data, cb }) => {
                    print!(" receive data: {}", std::str::from_utf8(&data).unwrap());
                    cbs.insert(id, cb);
                    node.node.propose(vec![], data).unwrap();
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

    let handle_messages = |msgs: Vec<Message>| {
        for _msg in msgs {
            // Send messages to other peers.
        }
    };

    if !ready.messages().is_empty() {
        // Send out the messages come from the node.
        handle_messages(ready.take_messages());
    }

    if !ready.snapshot().is_empty() {
        // This is a snapshot, we need to apply the snapshot at first.
        store.wl().apply_snapshot(ready.snapshot().clone()).unwrap();
    }

    let mut _last_apply_index = 0;
    let mut handle_committed_entries = |committed_entries: Vec<Entry>| {
        info!(logger, "handle_committed_entries"; "committed_entries" => format!("{:?}", committed_entries));
        for entry in committed_entries {

            if entry.data.is_empty() {
                // Empty entry, when the peer becomes Leader it will send an empty entry.
                continue;
            }

            if entry.get_entry_type() == EntryType::EntryNormal {
                let data = std::str::from_utf8(&entry.data).unwrap();
                let temp_data: Vec<&str> = data.split(" ").collect();
                let key = temp_data[0].to_owned();
                let value = temp_data[1].to_owned();
                info!(logger, "key {} value {}", key, value);
                node.key_value_store.insert(key, value);
                info!(logger, "handle_committed_entries"; "data" => data);
                if let Some(cb) = cbs.remove(entry.data.first().unwrap()) {
                    cb();
                }
            }

            // TODO: handle EntryConfChange
        }
    };
    handle_committed_entries(ready.take_committed_entries());

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
        handle_messages(ready.take_persisted_messages());
    }

    // Advance the Raft.
    let mut light_rd = node.node.advance(ready);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        store.wl().mut_hard_state().set_commit(commit);
    }
    // Send out the messages.
    handle_messages(light_rd.take_messages());
    // Apply all committed entries.
    handle_committed_entries(light_rd.take_committed_entries());
    // Advance the apply index.
    node.node.advance_apply();
}

pub fn send_propose(sender: mpsc::Sender<Msg>, key: String, value: String) {
        let logger = raft::default_logger();

        let (s1, r1) = mpsc::channel::<u8>();
        let data = format!("{} {}", key, value).into_bytes();

        // Send a command to the Raft, wait for the Raft to apply it
        // and get the result.
        sender
            .send(Msg::Propose {
                id: 1,
                data: data,
                cb: Box::new(move || {
                    s1.send(0).unwrap();
                }),
            })
            .unwrap();
        let n = r1.recv().unwrap();
        assert!(n == 1);
        info!(logger, "receive the propose callback"; "n" => n);
        if n == 0 {
            info!(logger, "receive the propose callback");
        }
}

