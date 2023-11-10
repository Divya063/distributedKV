mod raft_impl;
 
use actix_web::web::{Query, Json};
use bincode::de;
use slog::{Drain, Logger, Key};
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use raft_impl::*;
use std::error::Error;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder, ResponseError};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct ValRequest{
    key: String,
}

#[derive(Deserialize, Serialize)]
pub struct JoinRequest{
    id: u64,
}

#[derive(Deserialize)]
struct PayLoad {
    key: String,
    value: String,
}

struct KeyValueState {
    mailbox: Arc<Mutex<HashMap<u64, Sender<Msg>>>>,
    key_value_store: Arc<Mutex<HashMap<String, String>>>,
    logger: Logger,

}




#[actix_web::main]
async fn main() -> Result<(), Box<dyn Error>> {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    // maibox to send messages to the node
    let mailbox = Arc::new(Mutex::new(HashMap::new()));
    let (sender_leader, receiver_leader) = mpsc::channel();
    mailbox.lock().unwrap().insert(1, sender_leader.clone());

    // start leader node
    let kv_store = Arc::new(Mutex::new(HashMap::new()));
    let leader_node = Node::create_leader_node(&mailbox, receiver_leader, Arc::clone(&kv_store));
    let logger = leader_node.logger.clone();
    let node = Arc::new(Mutex::new(leader_node));
    let handler = tokio::spawn(raft_impl::start(node));

    // TODO: Spawn a task to wait for a termination signal and gracefully shutdown the server

    
    let server = HttpServer::new(move || {
        let logger = logger.clone();
        App::new()
        .app_data(web::Data::new(KeyValueState {
            mailbox: Arc::clone(&mailbox),
            key_value_store: Arc::clone(&kv_store),
            logger: logger.clone(),
        })
        )
            .route("/get", web::get().to(get_value))
            .route("/put", web::post().to(insert))
            .route("/join", web::get().to(join))
    })
    .bind("127.0.0.1:9023")?
    .run();

    let _ = tokio::join!(server, handler);
    Ok(())
}


async fn insert(data: web::Data<KeyValueState>, req_body: web::Json<PayLoad>) -> impl Responder {
    let mailbox = &*data.mailbox.lock().unwrap();
    let sender = mailbox.get(&1).unwrap().clone();
    let logger = data.logger.clone();
    // parse the request body
    let key = req_body.key.clone();
    let val = req_body.value.clone();
    raft_impl::send_propose(logger, sender, key, val).await;
    
   HttpResponse::Ok().body("Data inserted!")
}

async fn get_value(data: web::Data<KeyValueState>, query: web::Query<ValRequest>) -> impl Responder {
    let store = data.key_value_store.lock().unwrap();

    log::debug!("store {}", store.len());

    let key = query.key.clone();  
    if key == "" {
        return HttpResponse::BadRequest().body("Key cannot be empty");
    }
    let value = store.get(&key);
    log::debug!("value: {:?}", value);
    match value {
        Some(value) => {
            return HttpResponse::Ok().body("data: ".to_string() + &value);
        }
        None => {
            return HttpResponse::NotFound().body("Key not found");
        }
    };
}

async fn join(data: web::Data<KeyValueState>, query: web::Query<JoinRequest>) -> impl Responder {
    let id = query.id.clone();
    let mailbox = &mut data.mailbox.lock().unwrap();
    if mailbox.contains_key(&id) {
        return HttpResponse::BadRequest().body("Node already exists");
    }
    let logger = data.logger.clone();
    let sender = mailbox.get(&1).unwrap().clone();

    let (sender_follower, receiver_follower) = mpsc::channel();
    mailbox.insert(id, sender_follower);

    let follower_node = Node::create_follower_node(id, Arc::clone(&data.mailbox), receiver_follower, logger);
    let follower_node = Arc::new(Mutex::new(follower_node));
    let _follower_handler = tokio::spawn(raft_impl::start(follower_node));
    raft_impl::add_followers(id, sender);
    HttpResponse::Ok().finish()
}
