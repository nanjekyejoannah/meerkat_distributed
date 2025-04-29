use tokio;

mod tests;
mod comm;
mod frontend;
mod runtime; // runtime already contains manager.rs

use crate::runtime::manager; // Import manager correctly

#[tokio::main]
pub async fn main() {
    let mut communication = comm::Communication {
        client_stream_map: std::collections::HashMap::new(),
        manager: manager::Manager::new(), 
    };
    runtime::repl::repl().await;
     if let Err(e) = communication.process_remote().await {
        eprintln!("Error in WebSocket communication: {}", e);
    }
}
