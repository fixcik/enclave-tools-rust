use std::fs::{ self, File };

pub fn is_empty_file(path: &String) -> std::io::Result<bool> {
    let metadata = fs::metadata(path)?;
    let size = metadata.len();

    Ok(size == 0)
}

pub fn create_empty_file(path: &String) -> std::io::Result<File> {
    File::create(path)
}