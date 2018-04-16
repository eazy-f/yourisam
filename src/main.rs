use std::env;

static INDEX_EXT: &str = ".MYI";
static DATA_EXT: &str = ".MYD";

type MITableOptions = u64;

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
        "unknown".to_string()
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let directory = &args[1];
    let table_name = &args[2];
    let files = find_table_files(directory, table_name);
    let header = read_table_header(files.index);
    println!("{}", header.options.show());
}

fn find_table_files(directory: &String, table_name: &String) -> MITableFiles {
    let index_file = table_name.clone() + INDEX_EXT;
    let data_file = table_name.clone() + DATA_EXT;
    MITableFiles {
        index: directory.clone() + index_file.as_str(),
        data: directory.clone() + data_file.as_str()
    }
}

fn read_table_header(index_file: String) -> MITableHeader {
    MITableHeader {
        options: 0x0
    }
}
