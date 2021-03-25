use crate::server::{execute::SelectRequest, operators::Select, record::Record};
//use fst::{Map, MapBuilder};
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    sync::{mpsc::Receiver, Arc, RwLock},
    thread,
};

pub struct Block {
    //    index: fst::MapBuilder<RoaringBitmap>,
    pub index: HashMap<String, RoaringBitmap>,
    pub storage: Vec<Series>,
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
pub struct Series {
    id: usize,
    key: String,
    pub records: RwLock<Vec<Record>>,
}

impl Series {
    // Constructor.
    fn new(id: usize, key: String, record: Record) -> Self {
        Series {
            id: id,
            key: key,
            records: RwLock::new(vec![record]),
        }
    }

    // Insert a record into this series.
    fn insert(&self, record: Record) {
        let mut v = self.records.write().expect("RwLock poisoned");
        v.push(record);
    }
}

// Ingests a read operation.
fn db_read(read_rx: Receiver<SelectRequest>, shared_block: Arc<RwLock<Block>>) {
    // Receive read operations from the server
    for request in read_rx {
        let statement = request.statement.clone();
        println!("Received statement: {:?}", statement);
        let result = statement.eval(&shared_block);
        request.reply(result);
    }
}

// Ingests a write operation.
fn db_write(write_rx: Receiver<Record>, shared_block: Arc<RwLock<Block>>) {
    // Receive write operations from the server
    for received in write_rx {
        let key: String = received.get_key();
        let mut block = shared_block.write().expect("RwLock poisoned");
        // Check if this series exists in the block
        if let Some(id) = block.key_map.get(&key) {
            println!("Received a familiar key!");
            block.storage[*id].insert(received);
            continue;
        }
        // Key does not exist in the block
        println!("First time seeing this key.");
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
    let shared_block = Arc::new(RwLock::new(Block::new()));

    // Set up separate r/w threads so that read operations don't block writes
    let read_block = Arc::clone(&shared_block);
    let read_thr = thread::spawn(move || db_read(read_rx, read_block));
    let write_block = Arc::clone(&shared_block);
    let write_thr = thread::spawn(move || db_write(write_rx, write_block));
    read_thr.join().unwrap();
    write_thr.join().unwrap();
}
