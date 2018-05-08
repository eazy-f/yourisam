use std::env;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::{Result, SeekFrom, Error, ErrorKind};
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;

static INDEX_EXT: &str = ".MYI";
static DATA_EXT: &str = ".MYD";

type MITableOptions = u32;

const MI_OPTION_PACK_RECORD:     u32 = 1;
const MI_OPTION_PACK_KEYS:       u32 = 2;
const MI_OPTION_COMPRESS_RECORD: u32 = 4;

type BytePos = (usize, usize);

struct MIRecordBlock {
    record_len: Option<u32>,
    data_len: Option<u32>,
    unused_len: Option<u32>,
    next_filepos: Option<u64>,
    block_len: u32,
    deleted: bool
}

#[derive(Clone)]
#[derive(Copy)]
struct MIRecordBlockDef {
    record_len: Option<BytePos>,
    block_len: Option<BytePos>,
    data_len: Option<BytePos>,
    unused_len: Option<BytePos>,
    next_filepos: Option<BytePos>,
    header_len: u8,
    deleted: bool
}

struct MITableFiles {
    index: String,
    data: String
}

struct MIRecDef {
    rtype: i16,
    length: u16
}

struct MITableBase {
    records: Vec<MIRecDef>
}

struct MITableState {
    header: MITableHeader,
    base: MITableBase
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
    let block_types = record_block_definitions();
    let (tx, rx) = mpsc::channel();
    let writer = thread::spawn(move || read_table_records(&files, &block_types, tx).unwrap());
    write_records(rx);
}

fn find_table_files(directory: &String, table_name: &String) -> MITableFiles {
    let index_file = table_name.clone() + INDEX_EXT;
    let data_file = table_name.clone() + DATA_EXT;
    MITableFiles {
        index: directory.clone() + &index_file,
        data: directory.clone() + &data_file
    }
}

fn write_records(reader: Receiver<Vec<u8>>) {
    let mut messages = 0;
    for message in reader.iter() {
        messages += 1;
    }
    println!("read {} records", messages);
}

fn read_table_state(index_file: &String) -> Result<MITableState> {
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
        index.seek(SeekFrom::Current(keysegs * 18))?;
        keys -= 1;
        offset += keysegs * 18 + (read as i64);
    }
    let mut uniques = header.uniques;
    while 0 != uniques {
        let read = index.read(&mut uniquedef_buf)?;
        let keysegs = to_u32(&uniquedef_buf[0..2]) as i64;
        let mut keysegs_left = keysegs;
        while 0 != keysegs_left {
            index.read(&mut keyseg_buf)?;
            keysegs_left -= 1;
        }
        uniques -= 1;
        offset += keysegs * 18 + (read as i64);
    }
    let mut fields_left = fields;
    let mut fieldrec_buf = [0u8; 7];
    let mut recdefs = Vec::new();
    while 0 != fields_left {
        index.read(&mut fieldrec_buf);
        let recdef = MIRecDef {
            rtype: to_u32(&fieldrec_buf[0..2]) as i16,
            length: to_u32(&fieldrec_buf[2..4]) as u16
        };
        recdefs.push(recdef);
        fields_left -= 1;
    }
    Ok(MITableState{header: header, base: MITableBase{records: recdefs}})
}

fn to_u32(source: &[u8]) -> u32 {
    to_u64(source) as u32
}

fn to_u64(source: &[u8]) -> u64 {
    source.iter().fold(0, |acc, &b| acc*256 + b as u64)
}

fn read_table_records(files: &MITableFiles, block_types: &[Option<MIRecordBlockDef>],
                      writer: Sender<Vec<u8>>) -> Result<u64> {
    let mut records = 0;
    let state = read_table_state(&files.index)?;
    let mut table = File::open(&files.data)?;
    let mut block_type_buf = [0];
    const max_header_len: usize = 20;
    let mut block_header = [0; max_header_len];
    let mut position = 0u64;
    let mut saved_position = None;
    let mut result = Ok(records);
    let mut record = Vec::new();
    let mut record_pos: usize = 0;
    while 0 != table.read(&mut block_type_buf)? {
        let block_type = block_type_buf[0];
        match block_types[block_type as usize] {
            None => {
                let msg = format!("unknown block type: {}", block_type);
                result = Err(Error::new(ErrorKind::Other, msg));
                break;
            },
            Some(block_definition) => {
                let header_length = block_definition.header_len as u32;
                let header_size = header_length as usize;
                table.read(&mut block_header[0..header_size]);
                position += (block_type_buf.len() as u32 + header_length) as u64;
                let block_info = read_block_info(&block_header[0..header_size], &block_definition);
                println!("block at {:016x} type: {} len: {}", position, block_type, block_info.block_len);
                if block_info.record_len.is_some() {
                    let record_len = block_info.record_len.unwrap() as usize;
                    record_pos = 0;
                    record.resize(record_len, 0u8);
                }
                let should_read = block_info.data_len.is_some() && (block_info.record_len.is_some() || saved_position.is_some());
                if should_read {
                    let data_len = block_info.data_len.unwrap() as usize;
                    table.read(&mut record.as_mut_slice()[record_pos..(record_pos + data_len)]);
                    record_pos += data_len;
                    table.seek(SeekFrom::Current(-(data_len as i64)));
                }
                table.seek(SeekFrom::Current(block_info.block_len as i64));
                position += block_info.block_len as u64;
                if block_info.next_filepos.is_some() {
                    if should_read {
                        let next_pos = block_info.next_filepos.unwrap();
                        if saved_position.is_none() {
                            saved_position = Some(position);
                        }
                        position = next_pos;
                        table.seek(SeekFrom::Start(next_pos));
                    }
                } else if saved_position.is_some() {
                    let next_pos = saved_position.unwrap();
                    position = next_pos;
                    saved_position = None;
                    table.seek(SeekFrom::Start(next_pos));
                }
                if should_read && block_info.next_filepos.is_none() {
                    writer.send(record.clone());
                }
                records += 1;
                result = Ok(records)
            }
        }
    }
    result
}

fn read_block_info(header: &[u8], block_definition: &MIRecordBlockDef) -> MIRecordBlock {
    let record_len = find_header_bytes(header, block_definition.record_len);
    let data_len = find_header_bytes(header, block_definition.data_len);
    let unused_len = find_header_bytes(header, block_definition.unused_len);
    let block_len = if unused_len.is_some() {
        unused_len.unwrap() + data_len.unwrap()
    } else if !block_definition.deleted {
        data_len.unwrap()
    } else {
        let deleted_block_len = find_header_bytes(header, block_definition.block_len);
        deleted_block_len.unwrap() - (block_definition.header_len as u64)
    };
    let convert_to_u32 = |n| n as u32;
    MIRecordBlock {
        deleted: block_definition.deleted,
        block_len: block_len as u32,
        record_len: record_len.map(&convert_to_u32),
        data_len: data_len.map(&convert_to_u32),
        unused_len: unused_len.map(&convert_to_u32),
        next_filepos: find_header_bytes(header, block_definition.next_filepos)
    }
}

fn find_header_bytes(header: &[u8], position: Option<BytePos>) -> Option<u64> {
    position.map(|(start, end)| to_u64(&header[start..end]))
}

fn record_block_definitions() -> [Option<MIRecordBlockDef>; 256] {
    let mut definitions = [None; 256];
    definitions[0] = Some(MIRecordBlockDef {
        record_len: None,
        block_len: Some((0, 3)),
        data_len: None,
        unused_len: None,
        next_filepos: Some((3, 11)),
        header_len: 19,
        deleted: true
    });
    let small_full_pos = Some((0, 2));
    let big_full_pos = Some((0, 3));
    definitions[1] = Some(MIRecordBlockDef {
        record_len: small_full_pos,
        block_len: small_full_pos,
        data_len: small_full_pos,
        unused_len: None,
        next_filepos: None,
        header_len: 2,
        deleted: false
    });
    definitions[2] = Some(MIRecordBlockDef {
        record_len: big_full_pos,
        block_len: big_full_pos,
        data_len: big_full_pos,
        unused_len: None,
        next_filepos: None,
        header_len: 3,
        deleted: false
    });
    definitions[3] = Some(MIRecordBlockDef {
        record_len: small_full_pos,
        block_len: None,
        data_len: small_full_pos,
        unused_len: Some((2,3)),
        next_filepos: None,
        header_len: 3,
        deleted: false
    });
    definitions[4] = Some(MIRecordBlockDef {
        record_len: big_full_pos,
        block_len: None,
        data_len: big_full_pos,
        unused_len: Some((3,4)),
        next_filepos: None,
        header_len: 4,
        deleted: false
    });
    definitions[5] = Some(MIRecordBlockDef {
        record_len: small_full_pos,
        block_len: Some((2, 4)),
        data_len: Some((2, 4)),
        unused_len: None,
        next_filepos: Some((4, 12)),
        header_len: 12,
        deleted: false
    });
    definitions[7] = Some(MIRecordBlockDef {
        record_len: None,
        block_len: small_full_pos,
        data_len: small_full_pos,
        unused_len: None,
        next_filepos: None,
        header_len: 2,
        deleted: false
    });
    definitions[9] = Some(MIRecordBlockDef {
        record_len: None,
        block_len: None,
        data_len: small_full_pos,
        unused_len: Some((2, 3)),
        next_filepos: None,
        header_len: 3,
        deleted: false
    });
    definitions[11] = Some(MIRecordBlockDef {
        record_len: None,
        block_len: small_full_pos,
        data_len: small_full_pos,
        unused_len: None,
        next_filepos: Some((2, 10)),
        header_len: 10,
        deleted: false
    });
    definitions
}
