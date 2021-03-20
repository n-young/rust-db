use crate::server::{execute::SelectRequest, operators::Select, record::Record};
use std::{
    collections::HashMap,
    sync::{mpsc::Receiver, Arc, Mutex, RwLock},
    thread,
};

// Series struct.
struct Series {
    records: Mutex<Vec<Record>>,
}

impl Series {
    // Constructor.
    fn new(id: usize, record: Record) -> Self {
        Series {
            id: id,
            records: Mutex::new(vec![record]),
        }
    }

    // Insert a record into this series.
    fn insert(&self, record: Record) {
        let mut v = self.records.lock().unwrap();
        v.push(record);
    }
}

// Ingests a read operation.
fn db_read(read_rx: Receiver<SelectRequest>, index: Arc<RwLock<HashMap<String, Series>>>) {
    // Receive read operations from the server
    for request in read_rx {
        let statement = request.statement.clone();
        println!("Received statement: {:?}", statement);
        request.reply(Vec::new());
    }
}

// Ingests a write operation.
fn db_write(write_rx: Receiver<Record>, storage: Arc<RwLock<HashMap<String, Series>>>) {
    let mut id: usize = 0;
    let mut id_map: Vec<String> = Vec::new();
    // Receive write operations from the server
    for received in write_rx {
        let key: String = received.get_key();
        // Obtain a read lock on storage
        let map = storage.read().expect("RwLock poisoned");
        //
        if let Some(series) = map.get(&key) {
            println!("Received a familiar key!");
            series.insert(received);
            continue;
        }
        // Key does not exist in map
        println!("First time seeing this key.");
        // Replace read lock with a write lock
        drop(map);
        let mut map = storage.write().expect("RwLock poisoned");
        map.insert(key, Series::new(id.clone(), received));
        id_map.push(key);
        id += 1;
    }
}

//
pub fn db_open(read_rx: Receiver<SelectRequest>, write_rx: Receiver<Record>) {
    // Create an in-memory storage structure
    let index = Arc::new(RwLock::new(HashMap::new()));

    // Set up separate r/w threads so that read operations don't block writes
    let read_index = Arc::clone(&index);
    let read_thr = thread::spawn(move || db_read(read_rx, read_index));
    let write_index = Arc::clone(&index);
    let write_thr = thread::spawn(move || db_write(write_rx, write_index));
    read_thr.join().unwrap();
    write_thr.join().unwrap();
}
