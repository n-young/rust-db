extern crate bincode;
use crate::server::{execute::SelectRequest, record::Record};
use croaring::bitmap::Bitmap;
use chrono::{DateTime, Utc};
use dotenv::dotenv;
use serde::{Serialize, Deserialize};
use std::{collections::{HashMap, BTreeMap}, fs::{self, File}, io::Read, sync::{mpsc::Receiver, Arc, RwLock}, thread};

// CONSTANTS
const HEADER_SIZE: usize = 7;

// BlockIndex Struct.
// TODO: I think this is funky.. what is the actual typesig?
pub struct BlockIndex {
    index: BTreeMap<String, File>
}
impl BlockIndex {
    pub fn new() -> Self {
        // Create index, access dir.
        let mut index = BTreeMap::new();
        let dir = fs::read_dir(format!("{}/blocks", dotenv::var("DATAROOT").unwrap())).unwrap();

        // For each file in the dir...
        for f in dir {
            // Open the file and read it to a buffer.
            let mut file = File::open(f.unwrap().path().as_path()).unwrap();
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).unwrap();

            // Deserialize into a packed_block, get the timestamps.
            // TODO: Can just get from the file name LOL, then don't need to read?
            let packed_block = Block::from_bytes_packed(&buffer);
            let key = format!("{}_{}", packed_block.start_timestamp.unwrap(), packed_block.end_timestamp.unwrap());

            // Insert key into file.
            index.insert(key, file);
        }

        BlockIndex { index }
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
        let serialized_start_timestamp = self.start_timestamp.unwrap().to_rfc2822().into_bytes();
        let serialized_end_timestamp = self.end_timestamp.unwrap().to_rfc2822().into_bytes();
        let serialized_index_keys = bincode::serialize::<Vec<String>>(&self.index.keys().map(|x| x.clone()).collect()).unwrap();
        let serialized_index_values = bincode::serialize::<Vec<Vec<u8>>>(&self.index.values().map(|x| x.serialize()).collect()).unwrap();
        let serialized_id_map = bincode::serialize(&self.id_map).unwrap();
        let serialized_key_map = bincode::serialize(&self.key_map).unwrap();
        let serialized_storage = bincode::serialize(&self.storage).unwrap();
        let parts = vec![serialized_start_timestamp, serialized_end_timestamp, serialized_index_keys, serialized_index_values, serialized_id_map, serialized_key_map, serialized_storage];
        assert!(parts.len() == HEADER_SIZE);

        // Create a header of pointers.
        let mut cum: usize = 0;
        let mut header = vec![];
        for p in &parts {
            cum += p.len();
            header.push(cum as u8);
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
        let bytes_iter: Vec<u8> = bytes.into_iter().cloned().collect();
        let header: Vec<u8> = bytes_iter[0..HEADER_SIZE].to_vec();

        // Deserialize each segment.
        let bytes_start_timestamp = bytes_iter[HEADER_SIZE..header[0] as usize].to_vec();
        let deserialized_start_timestamp = DateTime::parse_from_rfc2822(&String::from_utf8(bytes_start_timestamp).unwrap()).unwrap().with_timezone(&Utc);
        
        let bytes_end_timestamp = bytes_iter[header[0] as usize..header[1] as usize].to_vec();
        let deserialized_end_timestamp = DateTime::parse_from_rfc2822(&String::from_utf8(bytes_end_timestamp).unwrap()).unwrap().with_timezone(&Utc);

        let bytes_index_keys: Vec<u8> = bytes_iter[header[1] as usize..header[2] as usize].to_vec();
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

    pub fn from_bytes_packed(bytes: &[u8]) -> PackedBlock {
        // Read header (should be fixed size) and figure out array segments
        let bytes_iter: Vec<u8> = bytes.into_iter().cloned().collect();
        let header: Vec<u8> = bytes_iter[0..HEADER_SIZE].to_vec();

        // Deserialize each segment.
        let bytes_start_timestamp = bytes_iter[HEADER_SIZE..header[0] as usize].to_vec();
        let deserialized_start_timestamp = DateTime::parse_from_rfc2822(&String::from_utf8(bytes_start_timestamp).unwrap()).unwrap().with_timezone(&Utc);
        
        let bytes_end_timestamp = bytes_iter[header[0] as usize..header[1] as usize].to_vec();
        let deserialized_end_timestamp = DateTime::parse_from_rfc2822(&String::from_utf8(bytes_end_timestamp).unwrap()).unwrap().with_timezone(&Utc);

        let bytes_index_keys: Vec<u8> = bytes_iter[header[1] as usize..header[2] as usize].to_vec();
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
fn db_read(read_rx: Receiver<SelectRequest>, shared_block: Arc<RwLock<Block>>) {// Receive read operations from the server
    for request in read_rx {
        let statement = request.statement.clone();
        println!("Received statement: {:?}", statement);
        let result = statement.eval(&shared_block);
        request.reply(result.into_vec());
    }
}

// Ingests a write operation.
fn db_write(write_rx: Receiver<Record>, shared_block: Arc<RwLock<Block>>) {
    // Receive write operations from the server
    for received in write_rx {
        // Get key and block.
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

        // After write, consider flushing.
        // TODO: Flush to disk every now and then. To write, use fs::write("path", data).expect("error")
    }
}

// Create block and open database.
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
