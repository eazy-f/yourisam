use std::env;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::{Result,SeekFrom};

static INDEX_EXT: &str = ".MYI";
static DATA_EXT: &str = ".MYD";

type MITableOptions = u32;

const MI_OPTION_PACK_RECORD:     u32 = 1;
const MI_OPTION_PACK_KEYS:       u32 = 2;
const MI_OPTION_COMPRESS_RECORD: u32 = 4;

struct MITableFiles {
    index: String,
    data: String
}

struct MITableHeader {
    options: MITableOptions,
    keys: u8,
    uniques: u8,
    key_parts: u32,
    unique_key_parts: u32,
    fulltext_keys: u8,
    base_pos: u32
}

trait Show {
    fn show(&self) -> String;
}

impl Show for MITableOptions {
    fn show(&self) -> String {
        let record_type = if self & MI_OPTION_PACK_RECORD != 0 {
            "dynamic"
        } else if self & MI_OPTION_COMPRESS_RECORD != 0 {
            "packed"
        } else {
            "static"
        };
        format!("{}", record_type.to_string())
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let directory = &args[1];
    let table_name = &args[2];
    let files = find_table_files(directory, table_name);
    match read_table_records(&files) {
        Ok(records) => println!("{}", records),
        Err(error) => println!("{}", error)
    }
}

fn find_table_files(directory: &String, table_name: &String) -> MITableFiles {
    let index_file = table_name.clone() + INDEX_EXT;
    let data_file = table_name.clone() + DATA_EXT;
    MITableFiles {
        index: directory.clone() + &index_file,
        data: directory.clone() + &data_file
    }
}

fn read_table_header(index_file: &String) -> Result<MITableHeader> {
    let mut index = File::open(index_file)?;
    let mut header_bytes = [0u8; 32];
    let mut keyseg_buf = [0u8; 18];
    let mut base_info_buf = [0u8; 100];
    index.read(&mut header_bytes)?;
    let header = MITableHeader {
        options: to_u32(&header_bytes[4..6]),
        base_pos: to_u32(&header_bytes[12..14]),
        key_parts: to_u32(&header_bytes[14..16]),
        unique_key_parts: to_u32(&header_bytes[16..18]),
        keys: header_bytes[18],
        uniques: header_bytes[19],
        fulltext_keys: header_bytes[22]
    };
    let mut keydef_buf = [0u8; 12];
    let mut uniquedef_buf = [0u8; 4];
    let mut keys = header.keys;
    let mut offset = header.base_pos as i64 + base_info_buf.len() as i64;
    index.seek(SeekFrom::Start(header.base_pos as u64))?;
    index.read(&mut base_info_buf)?;
    let fields = to_u32(&base_info_buf[64..68]);
    while 0 != keys {
        let read = index.read(&mut keydef_buf)?;
        let keysegs = keydef_buf[0] as i64;
        println!("keysegs: {} alg: {}", keysegs, keydef_buf[1]);
        index.seek(SeekFrom::Current(keysegs * 18))?;
        keys -= 1;
        offset += keysegs * 18 + (read as i64);
    }
    let mut uniques = header.uniques;
    while 0 != uniques {
        let read = index.read(&mut uniquedef_buf)?;
        let keysegs = to_u32(&uniquedef_buf[0..2]) as i64;
        println!("keysegs: {} key: {}", keysegs, uniquedef_buf[2]);
        let mut keysegs_left = keysegs;
        while 0 != keysegs_left {
            index.read(&mut keyseg_buf)?;
            println!("seg type: {} lang {}", keyseg_buf[0], keyseg_buf[1]);
            keysegs_left -= 1;
        }
        uniques -= 1;
        offset += keysegs * 18 + (read as i64);
    }
    let mut fields_left = fields;
    let mut fieldrec_buf = [0u8; 7];
    while 0 != fields_left {
        index.read(&mut fieldrec_buf);
        println!("field: {}, type: {}, length: {}",
                 fields - fields_left,
                 to_u32(&fieldrec_buf[0..2]),
                 to_u32(&fieldrec_buf[2..4]));
        fields_left -= 1;
    }
    println!("keys {} base pos {:x} offset {:x}", header.uniques, header.base_pos, offset);
    Ok(header)
}

fn to_u32(source: &[u8]) -> u32 {
    source.iter().fold(0, |acc, &b| acc*256 + b as u32)
}

fn read_table_records(files: &MITableFiles) -> Result<u64> {
    let mut records = 0;
    let header = read_table_header(&files.index)?;
    let mut table = File::open(&files.data)?;
    let mut block_type_buf = [0];
    let mut block_header = [0; 20];
    let mut position = 0;
    while 0 != table.read(&mut block_type_buf)? {
        let block_type = block_type_buf[0];
        let header_len = match block_type {
            0  => 20,
            1  => 3,
            2  => 4,
            3  => 4,
            4  => 5,
            5  => 13,
            6  => 15,
            7  => 3,
            8  => 4,
            9  => 4,
            10 => 5,
            11 => 11,
            12 => 12,
            13 => 16,
            _  => 3
        };
        table.read(&mut block_header[0..(header_len-1)]);
        let length_bytes = block_header_block_length_bytes(block_type, &block_header);
        let data_len = length_bytes.iter().fold(0, |acc, &b| acc*256 + b as u32);
        //println!("block at {:016x} type: {} len: {}", position, block_type, data_len);
        let offset = if block_type == 0 {
            0
        } else if block_type == 3 || block_type == 9 {
            (data_len + block_header[length_bytes.len()] as u32) as i64
        } else {
            data_len as i64
        };
        table.seek(SeekFrom::Current(offset));
        position += offset + (header_len as i64);
        records += 1;
    }
    Ok(records)
}

fn block_header_block_length_bytes(block_type: u8, header: &[u8]) -> &[u8] {
    let (start, end) = match block_type {
        0  => (0, 3),
        1  => (0, 2),
        2  => (0, 2),
        3  => (0, 2),
        4  => (0, 3),
        5  => (2, 4),
        6  => (3, 6),
        7  => (0, 2),
        8  => (0, 3),
        9  => (0, 2),
        10 => (0, 3),
        11 => (0, 2),
        12 => (0, 3),
        13 => (4, 7),
        _  => (0, 2)
    };
    &header[start..end]
}
