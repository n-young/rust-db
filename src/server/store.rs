extern crate bincode;
use crate::server::{execute::SelectRequest, record::Record, operators::process::dnf};
use croaring::bitmap::Bitmap;
use chrono::{DateTime, NaiveDateTime, Utc};
use dotenv;
use serde::{Serialize, Deserialize};
use std::{
    collections::{HashMap, BTreeMap},
    convert::TryInto,
    fs::{self, File},
    io::Read,
    mem::size_of,
    sync::{mpsc::Receiver, Arc, RwLock, RwLockWriteGuard},
    thread};
use uuid::Uuid;
use serde_json::{Result, Value};
use serde_json::json;

// CONSTANTS
// TODO: Put these in their own file
const HEADER_SIZE: usize = 7;
const FLUSH_FREQUENCY: u32 = 10;

// TODO: Break this file up.

// BlockIndex Struct.
pub struct BlockIndex {
    index: BTreeMap<i64, Vec<String>> // Map from start_timestamp (millis) to filename
}
impl BlockIndex {
    // Constructor.
    pub fn new() -> Self {
        let index = BTreeMap::new();
        BlockIndex { index }
    }

    // Insert into index.
    pub fn insert(&mut self, key: i64, filepath: String) {
        if self.index.contains_key(&key) {
            self.index.get_mut(&key).unwrap().push(filepath);
        } else {
            self.index.insert(key, vec![filepath]);
        }
    }

    // Constructor using path to data folder (should be specified in .env).
    pub fn from_disk(path: String) -> Self {
        let file = File::open(path);
        if let Ok(mut f) = file {
            let mut buffer = vec![];
            f.read_to_end(&mut buffer).expect("ERROR: issue reading index from disk.");
            let index = bincode::deserialize::<BTreeMap<i64, Vec<String>>>(&buffer).unwrap();
            BlockIndex { index }
        } else {
            BlockIndex::new()
        }
    }

    // Populate using elements in data folder (should not be used).
    pub fn populate_manually(&mut self) {
        // For each file in the dir...
        let filepath = format!("{}/blocks", dotenv::var("DATAROOT").unwrap());
        fs::create_dir_all(&filepath).expect("ERROR: issue creating block dir.");
        let dir = fs::read_dir(filepath).unwrap();
        for f in dir {
            // Get the filename.
            let filepath_pathbuf = f.unwrap().path();
            let filepath_path = filepath_pathbuf.as_path();
            let filepath = String::from(filepath_path.to_str().unwrap());

            // Unpack the given file.
            let packed_block = PackedBlock::from_filepath(filepath.clone());
            let key = packed_block.start_timestamp.unwrap().timestamp_millis();
            self.insert(key, filepath);
        }
    }

    // Add a block to the index, write new index to disk.
    pub fn update(&mut self, block: &mut RwLockWriteGuard<Block>) {
        // Get block bytes.
        let block_bytes = block.to_bytes();

        // Parse filename.
        let filepath = format!("{}/blocks", dotenv::var("DATAROOT").unwrap());
        let block_filename = format!("{}/{}.rdb", filepath, Uuid::new_v4().to_string());
        
        // Write bytes to filename.
        fs::create_dir_all(&filepath).expect("ERROR: issue creating block dir.");
        fs::write(&block_filename, block_bytes).expect("ERROR: writing block to disk");

        // Insert new block reference into index.
        self.insert(block.start_timestamp.unwrap().timestamp_millis(), block_filename);

        // Rewrite index to disk.
        let index_filename = format!("{}/index.rdb", dotenv::var("DATAROOT").unwrap());
        fs::write(&index_filename, bincode::serialize(&self.index).unwrap()).expect("ERROR: writing index to disk");

        // Print out block metadata.
        println!("Number of series: {}", block.storage.len());
        println!("Start timestamp: {}", block.start_timestamp.unwrap());
        println!("End timestamp: {}", block.end_timestamp.unwrap());

        // Flush block from memory.
        block.flush();
        println!("Flushed block to disk.");
    }

    // Get all blocked in packed form.
    pub fn get_packed_blocks(&self) -> Vec<PackedBlock> {
        // For each block in the index...
        let mut ret = vec![];
        for (_, v) in self.index.iter() {
            for f in v.iter() {
                // Unpack the given file.
                let packed_block = PackedBlock::from_filepath(f.clone());
                ret.push(packed_block);
            }
        }
        ret
    }

    // Get all blocks in a certain time range (by start times) in packed form.
    pub fn get_packed_blocked_range(&self, start_timestamp: DateTime<Utc>, end_timestamp: DateTime<Utc>) -> Vec<PackedBlock> {
        // For each block in the index...
        let mut ret = vec![];
        for (_, v) in self.index.range(start_timestamp.timestamp_millis()..end_timestamp.timestamp_millis()) {
            for f in v.iter() {
                // Unpack the given file.
                let packed_block = PackedBlock::from_filepath(f.clone());
                ret.push(packed_block);
            }
        }
        ret
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
    // Constructor.
    pub fn new() -> Self {
        Block {
            index: HashMap::new(),
            storage: vec![],
            id_map: vec![],
            key_map: HashMap::new(),
            start_timestamp: None,
            end_timestamp: None,
            frozen: false,
        }
    }

    // Return series.
    pub fn get_storage(&self) -> &Vec<Series> {
        &self.storage
    }

    // Get the bitmap for a specific label / metric.
    pub fn search_index(&self, key: String) -> Option<&Bitmap> {
        self.index.get(&key)
    }

    // Convert a block to bytes.
    pub fn to_bytes(&mut self) -> Vec<u8> {
        // Freeze and cap the block.
        self.frozen = true;
        
        // Serializing the block's parts.
        let serialized_start_timestamp = self.start_timestamp.unwrap().timestamp_millis().to_le_bytes().to_vec();
        let serialized_end_timestamp = self.end_timestamp.unwrap().timestamp_millis().to_le_bytes().to_vec();
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

    // Create a block from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        // Read header (should be fixed size) and figure out array segments
        let header_size = HEADER_SIZE * size_of::<usize>();
        let bytes_iter: Vec<u8> = bytes.into_iter().cloned().collect();
        let header: Vec<usize> = bytes_iter[0..header_size].to_vec().chunks(size_of::<usize>()).map(|x| usize::from_ne_bytes(x.try_into().unwrap())).collect();

        // Deserialize each segment.
        let bytes_start_timestamp: [u8; 8] = bytes_iter[header_size..header[0] as usize].try_into().unwrap();
        let start_millis = i64::from_le_bytes(bytes_start_timestamp);
        let start_secs = (start_millis / 1000) as i64;
        let start_nanos = ((start_millis % 1000) * 1_000_000) as u32;
        let deserialized_start_timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(start_secs, start_nanos), Utc);
        
        let bytes_end_timestamp: [u8; 8] = bytes_iter[header[0] as usize..header[1] as usize].try_into().unwrap();
        let end_millis = i64::from_le_bytes(bytes_end_timestamp);
        let end_secs = (end_millis / 1000) as i64;
        let end_nanos = ((end_millis % 1000) * 1_000_000) as u32;
        let deserialized_end_timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(end_secs, end_nanos), Utc);

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

    // Flush block data.
    pub fn flush(&mut self) {
        self.index = HashMap::new();
        self.storage = vec![];
        self.id_map = vec![];
        self.key_map = HashMap::new();
        self.start_timestamp = None;
        self.end_timestamp = None;
        self.frozen = false;
    }
}

// PackedBlock Struct
pub struct PackedBlock {
    start_timestamp: Option<DateTime<Utc>>,
    end_timestamp: Option<DateTime<Utc>>,
    index: HashMap<String, Bitmap>,
    filepath: String
}
impl PackedBlock {
    // Construct a PackedBlock from file.
    pub fn from_filepath(filepath: String) -> PackedBlock {
        // Read file out to bytes.
        let mut file = File::open(&filepath).expect("ERROR: invalid filepath.");
        let mut bytes = vec![];
        file.read_to_end(&mut bytes).expect("ERROR: issue reading block from disk.");
        
        // Read header (should be fixed size) and figure out array segments
        let header_size = HEADER_SIZE * size_of::<usize>();
        let bytes_iter: Vec<u8> = bytes.into_iter().collect();
        let header: Vec<usize> = bytes_iter[0..header_size].to_vec().chunks(size_of::<usize>()).map(|x| usize::from_ne_bytes(x.try_into().unwrap())).collect();

        // Deserialize each segment.
        let bytes_start_timestamp: [u8; 8] = bytes_iter[header_size..header[0] as usize].try_into().unwrap();
        let start_millis = i64::from_le_bytes(bytes_start_timestamp);
        let start_secs = (start_millis / 1000) as i64;
        let start_nanos = ((start_millis % 1000) * 1_000_000) as u32;
        let deserialized_start_timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(start_secs, start_nanos), Utc);
        
        let bytes_end_timestamp: [u8; 8] = bytes_iter[header[0] as usize..header[1] as usize].try_into().unwrap();
        let end_millis = i64::from_le_bytes(bytes_end_timestamp);
        let end_secs = (end_millis / 1000) as i64;
        let end_nanos = ((end_millis % 1000) * 1_000_000) as u32;
        let deserialized_end_timestamp = DateTime::from_utc(NaiveDateTime::from_timestamp(end_secs, end_nanos), Utc);

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
            filepath: filepath
        }
    }

    // Return the embedded Block.
    pub fn unpack(&self) -> Block {
        // Read file out to bytes.
        let mut file = File::open(&self.filepath).expect("ERROR: invalid filepath.");
        let mut bytes = vec![];
        file.read_to_end(&mut bytes).expect("ERROR: issue reading block from disk.");
        Block::from_bytes(&bytes)
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
        // Eval statement and reply.
        let statement = request.statement.clone();
        println!("===================================");
        println!("Received statement: {}", Value::to_string(&json!(statement)));

        // Convert to DNF.
        let dnf_statement = dnf(statement);
        println!("===================================");
        println!("Converted statement: {}", Value::to_string(&json!(dnf_statement)));

        let result = dnf_statement.eval(&shared_block);
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
        
        // TODO: Shouldn't this be in the Block impl?
        // Check if this series exists in the block
        if let Some(id) = block.key_map.get(&key) {
            block.storage[*id].insert(received.clone());
        }

        // Key does not exist in the block
        else {
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

        // Update block timeranges.
        if block.start_timestamp.is_none() || block.start_timestamp.unwrap() > received.clone().get_timestamp() {
            block.start_timestamp = Some(received.clone().get_timestamp().clone());
        }
        if block.end_timestamp.is_none() || block.end_timestamp.unwrap() < received.clone().get_timestamp() {
            block.end_timestamp = Some(received.clone().get_timestamp().clone());
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
    // Create an in-memory index, populated from disk.
    fs::create_dir_all(format!("{}", dotenv::var("DATAROOT").unwrap())).expect("ERROR: issue creating data dir.");
    let index = BlockIndex::from_disk(format!("{}/index.rdb", dotenv::var("DATAROOT").unwrap()));

    // Alternatively, populate it manually from the blocks in the dir.
    // let mut index = BlockIndex::new();
    // index.populate_manually();

    // Create shared in-memory storage structures.
    let shared_block = Arc::new(RwLock::new(Block::new()));
    let shared_index = Arc::new(RwLock::new(index));

    // Set up separate r/w threads so that read operations don't block writes
    let read_block = Arc::clone(&shared_block);
    let read_index = Arc::clone(&shared_index);
    let read_thr = thread::spawn(move || db_read(read_rx, read_block, read_index));

    let write_block = Arc::clone(&shared_block);
    let write_index = Arc::clone(&shared_index);
    let write_thr = thread::spawn(move || db_write(write_rx, write_block, write_index));

    // Join threads.
    read_thr.join().unwrap();
    write_thr.join().unwrap();
}
