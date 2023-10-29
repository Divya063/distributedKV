mod raft_impl;

use raft::prelude::{Entry, EntryType};
use std::{sync::mpsc::{channel, RecvTimeoutError}, time::{Instant, Duration}};
use std::thread;
use raft_impl::Node;
use std::sync::mpsc;
use regex::Regex;

enum Msg {
    Propose {
        id: u8,
        data: Vec<u8>,
    },
}

fn main() {
    let mut node = Node::create_leader_node();
  
    let (tx, rx) = channel();
    let timeout = Duration::from_millis(100);
    let mut remaining_timeout = timeout;

    let sender_thread = thread::spawn(move || loop {
        let tx_clone = tx.clone();
        send_propose(tx_clone, "Hello".to_string(), "World".to_string());
    });

   
    let handle_1 = thread::spawn(move || loop {
    let now = Instant::now();
    let message = rx.recv_timeout(remaining_timeout);
    println!("{}", message.is_ok());
    match message  {
        Ok(Msg::Propose{id,data}) => {
            // Let's save this for later.
            let result = node.node.propose(vec![], data);
        },
        Err(RecvTimeoutError::Timeout) => (),
        Err(RecvTimeoutError::Disconnected) => unimplemented!(),
    }

    let elapsed = now.elapsed();
    if elapsed >= remaining_timeout {
        remaining_timeout = timeout;
        
        node.node.tick();
    } else {
        remaining_timeout -= elapsed;
    }

    on_ready(&mut node);
});

handle_1.join().unwrap();
sender_thread.join().unwrap();

}


fn on_ready(node: &mut Node) {
    if !node.node.has_ready() {
        // Nothing to do.
        return;
    }

    let mut ready = node.node.ready();
    let committed_entries = ready.take_committed_entries();
    if !committed_entries.is_empty() {
        handle_committed_entries(node, committed_entries);
    }

   
}

pub fn handle_committed_entries(node: &mut Node, entries : Vec<Entry>) {
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
            }
        }

    }
}
// call propose to add the request to the Raft log explicitly.
fn send_propose(sender: mpsc::Sender<Msg>, key : String, value : String) {
        let data = format!("put {} {}", key, value).into_bytes();

        // Send a command to the Raft, wait for the Raft to apply it
        // and get the result.
        println!("Message sent to Raft.");
        sender
            .send(Msg::Propose{
                id: 1,
                data: data,
                })
            .unwrap();

        println!("receive the propose callback");
}