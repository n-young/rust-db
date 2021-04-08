mod client;
mod error;
mod server;

use dotenv::dotenv;
use std::env;

fn main() {
    dotenv().ok();
    
    let args: Vec<String> = env::args().collect();
    if args[1] == "client" {
        client::from_stdin()
    } else if args[1] == "server" {
        server::server()
    }
}
