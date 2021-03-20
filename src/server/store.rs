use crate::server::{execute::SelectRequest, operators::Select, record::Record};
//use fst::{Map, MapBuilder};
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    sync::{mpsc::Receiver, Arc, Mutex, RwLock},
    thread,
};

struct Block {
    //    index: fst::MapBuilder<RoaringBitmap>,
    index: HashMap<String, RoaringBitmap>,
    storage: Vec<Series>,
    id_map: Vec<String>,
    key_map: HashMap<String, usize>,
}

impl Block {
    fn new() -> Self {
        Block {
            //            index: fst::MapBuilder::memory(),
            index: HashMap::new(),
            storage: vec![],
            id_map: vec![],
            key_map: HashMap::new(),
        }
    }
}

// Series struct.
struct Series {
    id: usize,
    key: String,
    records: Mutex<Vec<Record>>,
}

impl Series {
    // Constructor.
    fn new(id: usize, key: String, record: Record) -> Self {
        Series {
            id: id,
            key: key,
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
fn db_read(read_rx: Receiver<SelectRequest>, index: Arc<RwLock<Block>>) {
    // Receive read operations from the server
    for request in read_rx {
        let statement = request.statement.clone();
        println!("Received statement: {:?}", statement);
        request.reply(Vec::new());
    }
}

// Ingests a write operation.
fn db_write(write_rx: Receiver<Record>, storage: Arc<RwLock<Block>>) {
    // Receive write operations from the server
    for received in write_rx {
        let key: String = received.get_key();
        let mut block = storage.write().expect("RwLock poisoned");
        //
        if let Some(id) = block.key_map.get(&key) {
            println!("Received a familiar key!");
            block.storage[*id].insert(received);
            continue;
        }
        // Key does not exist in map
        println!("First time seeing this key.");
        // Replace read lock with a write lock
        let id = block.storage.len();
        block
            .storage
            .push(Series::new(id.clone(), key.clone(), received.clone()));
        block.id_map.push(key.clone());
        block.key_map.insert(key, id.clone());
        // Insert label into the fst
        let labels = received.get_labels();
        for label in labels {
            // Check if label is in fst
            match block.index.get_mut(&label) {
                Some(rb) => {
                    rb.insert(id as u32);
                }
                None => {
                    let mut new_rb = RoaringBitmap::new();
                    new_rb.insert(id as u32);
                    block.index.insert(label, new_rb);
                }
            }
            // If so, get bitset and add id to ibtset
            // if not, add label to fst, create bitset, and init with id
            //            let bitset = block.index.insert(labe);
        }
    }
}

//
pub fn db_open(read_rx: Receiver<SelectRequest>, write_rx: Receiver<Record>) {
    // Create an in-memory storage structure
    let index = Arc::new(RwLock::new(Block::new()));

    // Set up separate r/w threads so that read operations don't block writes
    let read_index = Arc::clone(&index);
    let read_thr = thread::spawn(move || db_read(read_rx, read_index));
    let write_index = Arc::clone(&index);
    let write_thr = thread::spawn(move || db_write(write_rx, write_index));
    read_thr.join().unwrap();
    write_thr.join().unwrap();
}
