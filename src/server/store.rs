extern crate bincode;
use crate::server::{execute::SelectRequest, record::Record};
use croaring::bitmap::Bitmap;
use chrono::{DateTime, NaiveDateTime, Utc};
use dotenv;
use serde::{Serialize, Deserialize};
use std::{
    collections::{HashMap, BTreeMap},
    fs::{self, File},
    io::Read,
    mem::size_of,
    sync::{mpsc::Receiver, Arc, RwLock, RwLockWriteGuard},
    thread};
use std::convert::TryInto;

// CONSTANTS
const HEADER_SIZE: usize = 7;
const FLUSH_FREQUENCY: u32 = 10;

// BlockIndex Struct.
pub struct BlockIndex {
    // Map from start_timestamp to filename
    index: BTreeMap<i64, String>
}
impl BlockIndex {
    pub fn new() -> Self {
        let index = BTreeMap::new();
        BlockIndex { index }
    }

    pub fn from_disk(path: String) -> Self {
        let file = File::open(path);
        if let Ok(mut f) = file {
            let mut buffer = vec![];
            f.read_to_end(&mut buffer).expect("ERROR: issue reading index from disk.");
            let index = bincode::deserialize::<BTreeMap<i64, String>>(&buffer).unwrap();
            BlockIndex { index }
        } else {
            BlockIndex::new()
        }
    }

    pub fn populate_manually(&mut self) {
        // For each file in the dir...
        let dir = fs::read_dir(format!("{}/blocks", dotenv::var("DATAROOT").unwrap())).unwrap();
        for f in dir {
            // Get the filename, get the key, insert into the Index.
            let filepath_a = f.unwrap().path();
            let filepath = filepath_a.as_path();
            let filename = String::from(filepath.to_str().unwrap());
            let key = filename[0..filename.find("_").unwrap()].parse::<i64>().unwrap();
            self.index.insert(key, filename);
        }
    }

    pub fn update(&mut self, block: &mut RwLockWriteGuard<Block>) {
        // Get block bytes.
        let block_bytes = block.to_bytes();

        // Parse filename.
        let block_filename = format!("{}/blocks/{}_{}.rdb", dotenv::var("DATAROOT").unwrap(), block.start_timestamp.unwrap().timestamp(), block.end_timestamp.unwrap().timestamp());

        // Write bytes to filename.
        fs::write(&block_filename, block_bytes).expect("ERROR: writing block to disk");

        // Insert new block reference into index.
        self.index.insert(block.start_timestamp.unwrap().timestamp(), block_filename);

        // Rewrite index to disk.
        let index_filename = format!("{}/index.rdb", dotenv::var("DATAROOT").unwrap());
        fs::write(&index_filename, bincode::serialize(&self.index).unwrap()).expect("ERROR: writing index to disk");

        // Flush block from memory.
        block.flush();
        println!("Flushed block to disk.");
    }
}

// Block Struct.
pub struct Block {
    index: HashMap<String, Bitmap>,
    storage: Vec<Series>,
    id_map: Vec<String>,
    key_map: HashMap<String, usize>,
    start_timestamp: Option<DateTime<Utc>>,
    end_timestamp: Option<DateTime<Utc>>,
    frozen: bool,
}
impl Block {
    pub fn new() -> Self {
        Block {
            index: HashMap::new(),
            storage: vec![],
            id_map: vec![],
            key_map: HashMap::new(),
            start_timestamp: Some(Utc::now()),
            end_timestamp: None,
            frozen: false,
        }
    }

    pub fn get_storage(&self) -> &Vec<Series> {
        &self.storage
    }

    pub fn search_index(&self, key: String) -> Option<&Bitmap> {
        self.index.get(&key)
    }

    pub fn to_bytes(&mut self) -> Vec<u8> {
        // Freeze and cap the block.
        self.frozen = true;
        self.end_timestamp = Some(Utc::now());
        
        // Start serializing the block's parts.
        let serialized_start_timestamp = self.start_timestamp.unwrap().timestamp().to_le_bytes().to_vec();
        let serialized_end_timestamp = self.end_timestamp.unwrap().timestamp().to_le_bytes().to_vec();
        let serialized_index_keys = bincode::serialize::<Vec<String>>(&self.index.keys().map(|x| x.clone()).collect()).unwrap();
        let serialized_index_values = bincode::serialize::<Vec<Vec<u8>>>(&self.index.values().map(|x| x.serialize()).collect()).unwrap();
        let serialized_id_map = bincode::serialize(&self.id_map).unwrap();
        let serialized_key_map = bincode::serialize(&self.key_map).unwrap();
        let serialized_storage = bincode::serialize(&self.storage).unwrap();
        let parts = vec![serialized_start_timestamp, serialized_end_timestamp, serialized_index_keys, serialized_index_values, serialized_id_map, serialized_key_map, serialized_storage];
        assert!(parts.len() == HEADER_SIZE);

        // Create a header of pointers.
        let mut cum: usize = HEADER_SIZE * size_of::<usize>();
        let mut header = vec![];
        for p in &parts {
            cum += p.len();
            header.append(&mut cum.to_ne_bytes().to_vec());
        }

        // Construct and return.
        let mut data: Vec<u8> = vec![];
        data.append(&mut header);
        for mut p in parts {
            data.append(&mut p);
        }
        data
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        // Read header (should be fixed size) and figure out array segments
        let header_size = HEADER_SIZE * size_of::<usize>();
        let bytes_iter: Vec<u8> = bytes.into_iter().cloned().collect();
        let header: Vec<usize> = bytes_iter[0..header_size].to_vec().chunks(size_of::<usize>()).map(|x| usize::from_ne_bytes(x.try_into().unwrap())).collect();

        // Deserialize each segment.
        let bytes_start_timestamp: [u8; 8] = bytes_iter[header_size..header[0] as usize].try_into().unwrap();
        let deserialized_start_timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(i64::from_le_bytes(bytes_start_timestamp), 0), Utc);
        
        let bytes_end_timestamp: [u8; 8] = bytes_iter[header[0] as usize..header[1] as usize].try_into().unwrap();
        let deserialized_end_timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(i64::from_le_bytes(bytes_end_timestamp), 0), Utc);

        let bytes_index_keys: Vec<u8> = bytes_iter[header[1] as usize..header[2] as usize].try_into().unwrap();
        let deserialized_index_keys = bincode::deserialize::<Vec<String>>(&bytes_index_keys).unwrap();

        let bytes_index_values: Vec<u8> = bytes_iter[header[2] as usize..header[3] as usize].to_vec();
        let deserialized_index_values = bincode::deserialize::<Vec<Vec<u8>>>(&bytes_index_values).unwrap().iter().map(|x| Bitmap::deserialize(x)).collect::<Vec<Bitmap>>();
        
        let bytes_id_map: Vec<u8> = bytes_iter[header[3] as usize..header[4] as usize].to_vec();
        let deserialized_id_map = bincode::deserialize::<Vec<String>>(&bytes_id_map).unwrap();
        
        let bytes_key_map: Vec<u8> = bytes_iter[header[4] as usize..header[5] as usize].to_vec();
        let deserialized_key_map = bincode::deserialize::<HashMap<String, usize>>(&bytes_key_map).unwrap();
        
        let bytes_storage: Vec<u8> = bytes_iter[header[5] as usize..header[6] as usize].to_vec();
        let deserialized_storage = bincode::deserialize::<Vec<Series>>(&bytes_storage).unwrap();

        // Should be empty.
        assert!(bytes_iter.len() == header[6] as usize);

        // Create Hashmap
        let deserialized_index: HashMap<String, Bitmap> = (deserialized_index_keys.iter().cloned().zip(deserialized_index_values)).collect();

        // Initialize and return block.
        Block {
            index: deserialized_index,
            storage: deserialized_storage,
            id_map: deserialized_id_map,
            key_map: deserialized_key_map,
            start_timestamp: Some(deserialized_start_timestamp),
            end_timestamp: Some(deserialized_end_timestamp),
            frozen: false,
        }
    }

    pub fn flush(&mut self) {
        self.index = HashMap::new();
        self.storage = vec![];
        self.id_map = vec![];
        self.key_map = HashMap::new();
        self.start_timestamp = Some(Utc::now());
        self.end_timestamp = None;
        self.frozen = false;
    }
}

// PackedBlock Struct
pub struct PackedBlock {
    start_timestamp: Option<DateTime<Utc>>,
    end_timestamp: Option<DateTime<Utc>>,
    index: HashMap<String, Bitmap>,
    data: Vec<u8>,
}
impl PackedBlock {
    pub fn from_bytes(bytes: &[u8]) -> PackedBlock {
        // Read header (should be fixed size) and figure out array segments
        let header_size = HEADER_SIZE * size_of::<usize>();
        let bytes_iter: Vec<u8> = bytes.into_iter().cloned().collect();
        let header: Vec<usize> = bytes_iter[0..header_size].to_vec().chunks(size_of::<usize>()).map(|x| usize::from_ne_bytes(x.try_into().unwrap())).collect();

        // Deserialize each segment.
        let bytes_start_timestamp: [u8; 8] = bytes_iter[header_size..header[0] as usize].try_into().unwrap();
        let deserialized_start_timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(i64::from_le_bytes(bytes_start_timestamp), 0), Utc);
        
        let bytes_end_timestamp: [u8; 8] = bytes_iter[header[0] as usize..header[1] as usize].try_into().unwrap();
        let deserialized_end_timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(i64::from_le_bytes(bytes_end_timestamp), 0), Utc);

        let bytes_index_keys: Vec<u8> = bytes_iter[header[1] as usize..header[2] as usize].try_into().unwrap();
        let deserialized_index_keys = bincode::deserialize::<Vec<String>>(&bytes_index_keys).unwrap();

        let bytes_index_values: Vec<u8> = bytes_iter[header[2] as usize..header[3] as usize].to_vec();
        let deserialized_index_values = bincode::deserialize::<Vec<Vec<u8>>>(&bytes_index_values).unwrap().iter().map(|x| Bitmap::deserialize(x)).collect::<Vec<Bitmap>>();

        // Create Hashmap
        let deserialized_index: HashMap<String, Bitmap> = (deserialized_index_keys.iter().cloned().zip(deserialized_index_values)).collect();

        // Initialize and return block.
        PackedBlock {
            index: deserialized_index,
            start_timestamp: Some(deserialized_start_timestamp),
            end_timestamp: Some(deserialized_end_timestamp),
            data: bytes.iter().cloned().collect()
        }
    }

    pub fn unpack(&self) -> Block {
        Block::from_bytes(&self.data)
    }
}

// Series struct.
#[derive(Serialize, Deserialize)]
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

    // Convert to bytes.
    pub fn into_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    // Convert from bytes.
    pub fn from_bytes(data: &[u8]) -> Self {
        bincode::deserialize(data).unwrap()
    }
}

// Ingests a read operation.
fn db_read(read_rx: Receiver<SelectRequest>, shared_block: Arc<RwLock<Block>>, shared_index: Arc<RwLock<BlockIndex>>) {// Receive read operations from the server
    for request in read_rx {
        println!("{:?}", shared_index.read().unwrap().index);
        let statement = request.statement.clone();
        println!("Received statement: {:?}", statement);
        let result = statement.eval(&shared_block);
        request.reply(result.into_vec());
    }
}

// Ingests a write operation.
fn db_write(write_rx: Receiver<Record>, shared_block: Arc<RwLock<Block>>, shared_index: Arc<RwLock<BlockIndex>>) {
    // Counter for number of writes. NOTE: Temporary.
    let mut counter = 0;

    // Receive write operations from the server
    for received in write_rx {
        // Get key and block.
        let key: String = received.get_key();
        let mut block = shared_block.write().expect("RwLock poisoned");

        // Check if this series exists in the block
        if let Some(id) = block.key_map.get(&key) {
            println!("Received a familiar key!");
            block.storage[*id].insert(received);
        }

        // Key does not exist in the block
        else {
            println!("First time seeing this key.");
            let id = block.storage.len();
            block
                .storage
                .push(Series::new(id.clone(), key.clone(), received.clone()));
            block.id_map.push(key.clone());
            block.key_map.insert(key, id.clone());

            // Insert label key-value pairs and metrics into the fst
            let mut labels = received.get_labels();
            labels.append(&mut received.get_metrics());
            for label in labels {
                match block.index.get_mut(&label) {
                    Some(rb) => {
                        rb.add(id as u32);
                    }
                    None => {
                        let mut new_rb = Bitmap::create();
                        new_rb.add(id as u32);
                        block.index.insert(label, new_rb);
                    }
                }
            }

        }
        // After write, consider flushing.
        counter = counter + 1;
        if counter % FLUSH_FREQUENCY == 0 {
            shared_index.write().expect("RwLock poisoined").update(&mut block);
        }
    }
}

// Create block and open database.
pub fn db_open(read_rx: Receiver<SelectRequest>, write_rx: Receiver<Record>) {
    // Create an in-memory storage structure
    let shared_block = Arc::new(RwLock::new(Block::new()));

    // Create an in-memory index, populated from disk.
    let index = BlockIndex::from_disk(format!("{}/index.rdb", dotenv::var("DATAROOT").unwrap()));
    let shared_index = Arc::new(RwLock::new(index));

    // Set up separate r/w threads so that read operations don't block writes
    let read_block = Arc::clone(&shared_block);
    let read_index = Arc::clone(&shared_index);
    let read_thr = thread::spawn(move || db_read(read_rx, read_block, read_index));

    let write_block = Arc::clone(&shared_block);
    let write_index = Arc::clone(&shared_index);
    let write_thr = thread::spawn(move || db_write(write_rx, write_block, write_index));

    read_thr.join().unwrap();
    write_thr.join().unwrap();
}
