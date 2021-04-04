use std::{
    io,
    net::{Shutdown, TcpListener, TcpStream},
    sync::mpsc::{channel, Sender},
    thread,
};
use bincode::{deserialize_from, serialize_into};
use crate::server::{
    execute::{execute, SelectRequest},
    record::Record,
    store::db_open,
};

// Process a vector of records into a String.
fn postprocess(result: Vec<Record>) -> String {
    let _ = result;
    String::from("Processed some records")
}

// Takes a new client connection and executes input.
fn handle_tcp_connection(
    mut stream: TcpStream,
    read_tx: Sender<SelectRequest>,
    write_tx: Sender<Record>,
) {
    let addr = stream.peer_addr().unwrap();
    while match deserialize_from::<_, String>(&mut stream) {
        Ok(data) => {
            if let Ok(op) = serde_json::from_str(&data) {
                let response = match execute(op, &read_tx, &write_tx) {
                    Ok(Some(result)) => postprocess(result),
                    Ok(None) => String::from("Operation completed"),
                    Err(error) => format!("Error: {}", error),
                };
                serialize_into(&mut stream, &response).unwrap();
                true
            } else {
                let response = format!("Unrecognized input: {}", &data);
                serialize_into(&mut stream, &response).unwrap();
                true
            }
        }
        Err(_) => false,
    } {}

    // Shut down the connection.
    println!("Terminating connection with {}", addr);
    match stream.shutdown(Shutdown::Both) {
        Ok(_) => println!("Connection terminated"),
        Err(err) => match err.kind() {
            io::ErrorKind::NotConnected => println!("Connection already terminated"),
            _ => panic!("Shutdown problem"),
        },
    }
}

// Opens the server.
pub fn server() {
    // Open the db and create read/write channels
    let (read_tx, read_rx) = channel();
    let (write_tx, write_rx) = channel();
    thread::spawn(move || db_open(read_rx, write_rx));

    // Start listening for new connections.
    let listener = TcpListener::bind("127.0.0.1:12345").unwrap();
    for stream in listener.incoming() {
        let write_tx_clone = write_tx.clone();
        let read_tx_clone = read_tx.clone();

        match stream {
            Err(e) => println!("failed: {}", e),
            Ok(stream) => {
                thread::spawn(move || handle_tcp_connection(stream, read_tx_clone, write_tx_clone));
            }
        }
    }
}
