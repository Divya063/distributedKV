mod raft_impl;

use raft::prelude::{Entry, EntryType};
use std::{sync::mpsc::{channel, RecvTimeoutError, RecvError}, time::{Instant, Duration}};
use std::thread;
use raft_impl::Node;
use std::sync::mpsc;
use regex::Regex;
use std::collections::HashMap;


type ProposeCallback = Box<dyn Fn() + Send>;

enum Msg {
    Propose {
        id: u8,
        data: Vec<u8>,
        callback: ProposeCallback,
    },
}
fn main() {
    let mut node = Node::create_leader_node();
  
    let (tx, rx) = channel();

    // Raft is a state machine, it will not change its state until the timeout.
    // so we need to call the tick function to change the state of the Raft.
    let timeout = Duration::from_millis(100);
    let mut remaining_timeout = timeout;

    let sender_thread = thread::spawn(move || loop {
        let tx_clone = tx.clone();
        send_propose(tx_clone, "Hello".to_string(), "World".to_string());
    });

   
    let handle_1 = thread::spawn(move || loop {
    let now = Instant::now();
    let message = rx.recv();
    // Use a HashMap to hold the `propose` callbacks.
    let mut cbs = HashMap::new();
    match message  {
        Ok(Msg::Propose{id,data, callback}) => {
            let last_index1 = node.node.raft.raft_log.last_index() + 1;
            cbs.insert(id, callback);
            let _result = node.node.propose(vec![], data);
            let last_index2 = node.node.raft.raft_log.last_index() + 1;
            if last_index2 == last_index1 {
                println!("propose failed");
            }
            
        },
        Err(RecvError) => (),
    }

    let elapsed = now.elapsed();
    if elapsed >= remaining_timeout {
        remaining_timeout = timeout;
        
        node.node.tick();
    } else {
        remaining_timeout -= elapsed;
    }

    on_ready(&mut node, &mut cbs);

});

handle_1.join().unwrap();
sender_thread.join().unwrap();

}


fn on_ready(node: &mut Node, cbs: &mut HashMap<u8, ProposeCallback>) {
    if !node.node.has_ready() {
        // Nothing to do.
        return;
    }

    println!("node {}", node.node.has_ready());

    let store = node.node.raft.raft_log.store.clone();

    let mut ready = node.node.ready();
    if !ready.snapshot().is_empty() {
        let s = ready.snapshot().clone();
        if let Err(e) = store.wl().apply_snapshot(s) {
            println!("apply snapshot fail: {:?}, need to retry or panic",e);
            return;
        }
    }
    let committed_entries = ready.take_committed_entries();
    println!("committed_entries {:?}", committed_entries);
    if !committed_entries.is_empty() {
        handle_committed_entries(node, committed_entries, cbs);
        
    }

    if let Some(hs) = ready.hs() {
        store.wl().set_hardstate(hs.clone());
    }

    // Advance the Raft.
    let mut light_rd = node.node.advance(ready);
    // Update commit index.
    if let Some(commit) = light_rd.commit_index() {
        store.wl().mut_hard_state().set_commit(commit);
    }

    handle_committed_entries(node, light_rd.take_committed_entries(), cbs);
    // Advance the apply index.
    node.node.advance_apply();

   
}

pub fn handle_committed_entries(node: &mut Node, entries : Vec<Entry>, cbs: &mut HashMap<u8, ProposeCallback>) {
    for entry in entries {
        if entry.data.is_empty() {
            // From new elected leaders.
            continue;
        }
        if let EntryType::EntryConfChange = entry.get_entry_type() {
            
        } else {
            let data = std::str::from_utf8(&entry.data).unwrap();
            let reg = Regex::new("put ([0-9]+) (.+)").unwrap();
            if let Some(caps) = reg.captures(data) {
                node.key_value_store.insert(caps[1].parse().unwrap(), caps[2].to_string());
                println!("cbs {}", entry.data.first().unwrap());
                if let Some(cb) = cbs.remove(entry.data.first().unwrap()) {
                    cb();
                }
            }
        }

    }
}
// call propose to add the request to the Raft log explicitly.
fn send_propose(sender: mpsc::Sender<Msg>, key : String, value : String) {
    thread::sleep(Duration::from_millis(1000));

        let (tx, rx) = mpsc::sync_channel::<bool>(1);

        let data = format!("put {} {}", key, value).into_bytes();

        // Send a command to the Raft, wait for the Raft to apply it
        // and get the result.
        println!("Message sent to Raft.");
        sender
            .send(Msg::Propose{
                id: 1,
                data: data,
                callback: Box::new(move || {
                    tx.send(true).unwrap();
                }),
                })
            .unwrap();
        // let n = rx.recv().unwrap();
        // assert_eq!(n, true);

        println!("receive the propose callback");
}