use std::cmp::Ordering;
use std::collections::{ HashSet, HashMap };
use std::{ fs::File };

use csv::{ Reader, ReaderBuilder, WriterBuilder, Writer, ByteRecord };

#[derive(PartialEq)]
pub enum MergeStrategy {
    Or,
    And,
    AndNot,
}

pub struct Merger {
    left_file_path: String,
    right_file_path: String,
    strategy: MergeStrategy,
    left_key: String,
    right_key: String,
    number_key: bool,
    output: String,
}

fn to_number(x: &[u8]) -> i64 {
    String::from_utf8(x.to_vec()).unwrap().parse::<i64>().unwrap()
}

impl Merger {
    pub fn new(
        left_file_path: String,
        right_file_path: String,
        strategy: MergeStrategy,
        left_key: String,
        right_key: String,
        number_key: bool,
        output: String
    ) -> Merger {
        Merger {
            left_file_path,
            right_file_path,
            strategy,
            left_key,
            right_key,
            output,
            number_key,
        }
    }

    pub fn handle(self) {
        let mut left_reader = self.get_left_reader();
        let mut right_reader = self.get_right_reader();

        let left_headers = self.get_headers(&mut left_reader);
        let right_headers = self.get_headers(&mut right_reader);
        let output_headers = self.get_output_headers(&left_headers, &right_headers);

        let map_left_headers_to_union = self.map_file_headers_to_output(
            &output_headers,
            &left_headers
        );

        let map_right_headers_to_union = self.map_file_headers_to_output(
            &output_headers,
            &right_headers
        );

        let mut writer = self.get_writer();
        writer.write_record(&output_headers).expect("write output headers");

        let left_col_index = self
            .get_header_pos(&right_headers, &self.left_key)
            .expect("Has column in left file");

        let right_col_index = self
            .get_header_pos(&right_headers, &self.right_key)
            .expect("Has column in right file");

        let mut left_lines = left_reader.into_byte_records();
        let mut right_lines = right_reader.into_byte_records();

        let mut left_line = left_lines.next();
        let mut right_line = right_lines.next();

        let mut old_left_value: Option<Vec<u8>> = None;

        let mut left_readed = true;
        let mut right_readed = true;

        let mut need_left_push;
        let mut old_left_eq_right;
        let mut need_read_left;

        let mut left_value;
        let mut right_value;

        let mut counter = 0;

        while let (Some(Ok(left_record)), Some(Ok(right_record))) = (&left_line, &right_line) {
            left_value = left_record.get(left_col_index).unwrap();
            right_value = right_record.get(right_col_index).unwrap();
            counter += 1;

            if counter % 100000 == 0 {
                println!("handled {} loops", counter);
            }

            let cmp = self.compare(&left_value, &right_value);

            old_left_eq_right = match &old_left_value {
                Some(v) => v == right_value,
                _ => false,
            };
            need_read_left = old_left_eq_right && cmp != Ordering::Equal;

            need_left_push = match self.strategy {
                MergeStrategy::And => left_readed && cmp == Ordering::Equal,
                MergeStrategy::Or => left_readed && !need_read_left,
                MergeStrategy::AndNot => left_readed && cmp == Ordering::Less && !need_read_left,
            };
            if need_left_push {
                // println!("left: {:?}", left_record);
                self.write_row(&mut writer, &map_left_headers_to_union, left_record);
                left_readed = false;
            }
            let need_right_push = match self.strategy {
                MergeStrategy::Or => right_readed,
                MergeStrategy::AndNot => false,
                MergeStrategy::And => right_readed && (cmp == Ordering::Equal || old_left_eq_right),
            };
            if need_right_push {
                // println!("right: {:?} ", right_record);
                self.write_row(&mut writer, &map_right_headers_to_union, right_record);
                right_readed = false;
            }

            if (cmp == Ordering::Less || cmp == Ordering::Equal) && !need_read_left {
                left_readed = true;
                // println!("read left");
                old_left_value = Some(left_value.to_owned());
                left_line = left_lines.next();
            } else {
                right_readed = true;
                // println!("read right");
                right_line = right_lines.next();
            }
        }

        if self.strategy != MergeStrategy::And {
            while let Some(Ok(left_record)) = &left_line {
                if left_readed {
                    match &self.strategy {
                        MergeStrategy::Or | MergeStrategy::AndNot => {
                            // println!("left: {:?}", left_record);
                            self.write_row(&mut writer, &map_left_headers_to_union, left_record);
                        }
                        _ => (),
                    }
                }

                left_line = left_lines.next();
                left_readed = true;
            }
        }

        if self.strategy != MergeStrategy::And {
            while let Some(Ok(right_record)) = &right_line {
                if right_readed {
                    match &self.strategy {
                        MergeStrategy::Or => {
                            // println!("right: {:?}", right_record);
                            self.write_row(&mut writer, &map_right_headers_to_union, right_record);
                        }
                        _ => (),
                    }
                }

                right_line = right_lines.next();
                right_readed = true;
            }
        }
    }

    fn get_writer(&self) -> Writer<File> {
        WriterBuilder::new().delimiter(b'\t').from_path(&self.output).expect("open output file")
    }

    fn get_left_reader(&self) -> Reader<File> {
        self.build_reader(&self.left_file_path)
    }
    fn get_right_reader(&self) -> Reader<File> {
        self.build_reader(&self.right_file_path)
    }

    fn build_reader(&self, path: &String) -> Reader<File> {
        ReaderBuilder::new().delimiter(b'\t').from_path(path).expect("Open file")
    }

    fn get_headers(&self, reader: &mut Reader<File>) -> Vec<String> {
        reader
            .headers()
            .expect("Left headers")
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn get_output_headers<'a>(
        &self,
        left_headers: &'a Vec<String>,
        right_headers: &'a Vec<String>
    ) -> Vec<String> {
        let left_headers_set: HashSet<&String> = left_headers.iter().collect();
        let right_headers_set: HashSet<&String> = right_headers.iter().collect();

        left_headers_set
            .union(&right_headers_set)
            .map(|&h| h.to_owned())
            .collect()
    }

    fn map_file_headers_to_output(
        &self,
        output_headers: &Vec<String>,
        file_headers: &Vec<String>
    ) -> HashMap<usize, Option<usize>> {
        output_headers
            .iter()
            .enumerate()
            .map(|(x, header)| (x, self.get_header_pos(file_headers, header)))
            .collect()
    }

    fn get_header_pos(&self, headers: &Vec<String>, name: &String) -> Option<usize> {
        headers.iter().position(|col| col == name)
    }

    fn write_row(
        &self,
        wrt: &mut Writer<File>,
        mapping: &HashMap<usize, Option<usize>>,
        record: &ByteRecord
    ) {
        for i in 0..mapping.len() {
            let rec_key = *mapping.get(&i).unwrap();
            if let Some(rec_key) = rec_key {
                wrt.write_field(&record[rec_key]).expect("write field");
            } else {
                wrt.write_field("").expect("write field");
            }
        }
        wrt.write_record(None::<&[u8]>).expect("write record");
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if self.number_key { to_number(a).cmp(&to_number(b)) } else { a.cmp(b) }
    }
}