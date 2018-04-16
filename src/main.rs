use std::env;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::Result;

static INDEX_EXT: &str = ".MYI";
static DATA_EXT: &str = ".MYD";

type MITableOptions = u64;

const MI_OPTION_PACK_RECORD:     u64 = 1;
const MI_OPTION_PACK_KEYS:       u64 = 2;
const MI_OPTION_COMPRESS_RECORD: u64 = 4;

struct MITableFiles {
    index: String,
    data: String
}

struct MITableHeader {
    options: MITableOptions
}

trait Show {
    fn show(&self) -> String;
}

impl Show for MITableOptions {
    fn show(&self) -> String {
        let record_type = if self & MI_OPTION_PACK_RECORD != 0 {
            "packed"
        } else if self & MI_OPTION_COMPRESS_RECORD != 0 {
            "compressed"
        } else {
            "empty"
        };
        format!("{}", record_type.to_string())
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let directory = &args[1];
    let table_name = &args[2];
    let files = find_table_files(directory, table_name);
    match read_table_header(files.index) {
        Ok(header) => println!("{}", header.options.show()),
        Err(error) => println!("{}", error)
    }
}

fn find_table_files(directory: &String, table_name: &String) -> MITableFiles {
    let index_file = table_name.clone() + INDEX_EXT;
    let data_file = table_name.clone() + DATA_EXT;
    MITableFiles {
        index: directory.clone() + index_file.as_str(),
        data: directory.clone() + data_file.as_str()
    }
}

fn read_table_header(index_file: String) -> Result<MITableHeader> {
    let mut index = File::open(index_file)?;
    let mut header_bytes = [0; 32];
    index.read(&mut header_bytes)?;
    let options = header_bytes[4..6].iter().fold(0, |acc, &b| acc*256 + b as u64);
    let header = MITableHeader {
        options: options
    };
    Ok(header)
}
