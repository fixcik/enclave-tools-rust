use std::cmp::Ordering;
use std::collections::{ HashMap, HashSet };
use std::fs::File;
use std::vec;

use csv::{ ByteRecord, ByteRecordsIntoIter, Error, Reader, ReaderBuilder, Writer, WriterBuilder };

use super::deduplicate::{ DeduplicateStrategy, Side };

#[derive(PartialEq)]
pub enum MergeStrategy {
    Or,
    And,
    AndNot,
}

pub struct Merger {
    left_file_path: String,
    right_file_path: String,
    merge_strategy: MergeStrategy,
    deduplicate_strategy: DeduplicateStrategy,
    left_key: String,
    right_key: String,
    number_key: bool,
    output: String,
}

fn to_number(x: &[u8]) -> i64 {
    let string = String::from_utf8(x.to_vec()).expect(format!("Parse string: {:?}", x).as_str());

    string.parse::<i64>().expect(format!("Parse number: {}", string).as_str())
}

impl Merger {
    pub fn create(
        left_file_path: String,
        right_file_path: String,
        merge_strategy: MergeStrategy,
        deduplicate_strategy: DeduplicateStrategy,
        left_key: String,
        right_key: String,
        number_key: bool,
        output: String
    ) -> Merger {
        Merger {
            left_file_path,
            right_file_path,
            merge_strategy,
            deduplicate_strategy,
            left_key,
            right_key,
            output,
            number_key,
        }
    }

    pub fn handle(self) -> Result<(), csv::Error> {
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

        let left_key_index = self
            .get_header_pos(&output_headers, &self.left_key)
            .expect(format!("Has column {} in left file", self.left_key).as_str());

        println!("{} has index {} in {:?}", self.left_key, left_key_index, output_headers);

        let right_key_index = self
            .get_header_pos(&output_headers, &self.right_key)
            .expect(format!("Has column {} in right file", self.right_key).as_str());

        println!("{} has index {} in {:?}", self.right_key, right_key_index, output_headers);

        let mut writer = self.get_writer();

        writer.write_record(&output_headers).expect("write output headers");

        let mut deduplicate_handler = DeduplicateStrategy::create(
            self.deduplicate_strategy,
            &mut writer,
            left_key_index,
            right_key_index
        );

        let mut left_lines = left_reader.into_byte_records();
        let mut right_lines = right_reader.into_byte_records();

        let mut left_line = self.read_record(&mut left_lines, &map_left_headers_to_union);
        let mut right_line = self.read_record(&mut right_lines, &map_right_headers_to_union);

        let mut old_left_value: Option<Vec<u8>> = None;

        let mut left_readed = true;
        let mut right_readed = true;

        let mut need_left_push;
        let mut need_right_push;
        let mut old_left_eq_right;
        let mut need_read_left;

        let mut left_value;
        let mut right_value;

        let mut counter = 0;
        while let (Some(left_record), Some(right_record)) = (&left_line, &right_line) {
            println!("{:?} {:?}", left_record, right_record);
            left_value = left_record.get(left_key_index).unwrap();
            right_value = right_record.get(right_key_index).unwrap();
            counter += 1;

            if counter % 100000 == 0 {
                println!("handled {} loops", counter);
            }

            let cmp = self.compare(&left_value, &right_value);

            old_left_eq_right = match &old_left_value {
                Some(v) => v == right_value,
                _ => false,
            };
            need_read_left = old_left_eq_right && cmp.is_ne();

            need_left_push = match self.merge_strategy {
                MergeStrategy::And => left_readed && cmp.is_eq(),
                MergeStrategy::Or => left_readed && !need_read_left,
                MergeStrategy::AndNot => left_readed && cmp == Ordering::Less && !need_read_left,
            };
            need_right_push = match self.merge_strategy {
                MergeStrategy::And => right_readed && (cmp.is_eq() || old_left_eq_right),
                MergeStrategy::Or => right_readed,
                MergeStrategy::AndNot => false,
            };
            if need_left_push {
                deduplicate_handler.add_row(left_record.clone(), Side::Left)?;
                // self.write_row(&mut writer, &map_left_headers_to_union, left_record);
                left_readed = false;
            }
            if need_right_push {
                // println!("right: {:?} ", right_record);
                deduplicate_handler.add_row(right_record.clone(), Side::Right)?;
                // self.write_row(&mut writer, &map_right_headers_to_union, right_record);
                right_readed = false;
            }

            if cmp.is_le() && !need_read_left {
                left_readed = true;
                // println!("read left");
                old_left_value = Some(left_value.to_owned());
                left_line = self.read_record(&mut left_lines, &map_left_headers_to_union);
            } else {
                right_readed = true;
                // println!("read right");
                right_line = self.read_record(&mut right_lines, &map_right_headers_to_union);
            }
        }

        if self.merge_strategy != MergeStrategy::And {
            while let Some(left_record) = &left_line {
                if left_readed {
                    match &self.merge_strategy {
                        MergeStrategy::Or | MergeStrategy::AndNot => {
                            // println!("left: {:?}", left_record);
                            deduplicate_handler.add_row(left_record.clone(), Side::Left)?;
                            // self.write_row(&mut writer, &map_left_headers_to_union, left_record);
                        }
                        _ => (),
                    }
                }

                left_line = self.read_record(&mut left_lines, &map_left_headers_to_union);
                left_readed = true;
            }
        }

        if self.merge_strategy != MergeStrategy::And {
            while let Some(right_record) = &right_line {
                if right_readed {
                    match &self.merge_strategy {
                        MergeStrategy::Or => {
                            // println!("right: {:?}", right_record);
                            deduplicate_handler.add_row(right_record.clone(), Side::Right)?;
                            // self.write_row(&mut writer, &map_right_headers_to_union, right_record);
                        }
                        _ => (),
                    }
                }

                right_line = self.read_record(&mut right_lines, &map_right_headers_to_union);
                right_readed = true;
            }
        }

        deduplicate_handler.flush()?;
        Ok(())
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
        ReaderBuilder::new()
            .delimiter(b'\t')
            .from_path(path)
            .expect(format!("Open file: {}", path).as_str())
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
        let mut result: Vec<String> = vec![];

        for header in left_headers {
            result.push(header.to_owned());
        }

        for header in right_headers {
            result.push(header.to_owned());
        }

        let mut set = HashSet::new();
        result
            .into_iter()
            .filter(|x| set.insert(x.clone()))
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

    fn read_record(
        &self,
        iter: &mut ByteRecordsIntoIter<File>,
        mapping: &HashMap<usize, Option<usize>>
    ) -> Option<ByteRecord> {
        if let Some(Ok(record)) = iter.next() {
            println!("rec: {:?}", record);
            let mut values: Vec<&[u8]> = Vec::with_capacity(mapping.len());
            for i in 0..mapping.len() {
                let rec_key = *mapping.get(&i).unwrap();
                if let Some(rec_key) = rec_key {
                    values.push(&record[rec_key]);
                } else {
                    values.push(b"");
                }
            }
            return Some(ByteRecord::from_iter(&values));
        }
        None
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if self.number_key { to_number(a).cmp(&to_number(b)) } else { a.cmp(b) }
    }
}