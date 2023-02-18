use std::cmp::Ordering;
use std::collections::{ HashMap, HashSet };
use std::fs::File;
use std::vec;

use csv::{ ByteRecord, ByteRecordsIntoIter, Reader, ReaderBuilder, Writer, WriterBuilder };
use napi::threadsafe_function::{ ThreadsafeFunction, ErrorStrategy };

use futures::executor;

use crate::{ MergeStrategy, DeduplicateStrategy };

use super::deduplicate::{ Side };

pub struct Merger {
    left_file_path: String,
    right_file_path: String,
    merge_strategy: MergeStrategy,
    deduplicate_strategy: DeduplicateStrategy,
    left_key: String,
    right_key: String,
    number_key: bool,
    output: String,
    output_header_callback: Option<ThreadsafeFunction<String, ErrorStrategy::Fatal>>,
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
        output: String,
        output_header_callback: Option<ThreadsafeFunction<String, ErrorStrategy::Fatal>>
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
            output_header_callback,
        }
    }

    pub fn handle(self) -> Result<(), csv::Error> {
        let mut left_reader = self.get_left_reader()?;
        let mut right_reader = self.get_right_reader()?;

        let left_headers = self.get_headers(&mut left_reader)?;
        let right_headers = self.get_headers(&mut right_reader)?;
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
            .get_formatted_header_position(&output_headers, &self.left_key)
            .expect(format!("Has column {} in left file", self.left_key).as_str());

        let right_key_index = self
            .get_formatted_header_position(&output_headers, &self.right_key)
            .expect(format!("Has column {} in right file", self.right_key).as_str());

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
                MergeStrategy::Or => left_readed && cmp.is_le() && !need_read_left,
                MergeStrategy::AndNot => left_readed && cmp == Ordering::Less && !need_read_left,
            };
            need_right_push = match self.merge_strategy {
                MergeStrategy::And => right_readed && (cmp.is_eq() || old_left_eq_right),
                MergeStrategy::Or => right_readed && cmp.is_ge(),
                MergeStrategy::AndNot => false,
            };

            if need_left_push {
                deduplicate_handler.add_row(left_record.clone(), Side::Left)?;
                // self.write_row(&mut writer, &map_left_headers_to_union, left_record);
                left_readed = false;
            }
            if need_right_push {
                deduplicate_handler.add_row(right_record.clone(), Side::Right)?;
                // self.write_row(&mut writer, &map_right_headers_to_union, right_record);
                right_readed = false;
            }

            if cmp.is_le() && !need_read_left {
                left_readed = true;
                old_left_value = Some(left_value.to_owned());
                left_line = self.read_record(&mut left_lines, &map_left_headers_to_union);
            } else {
                right_readed = true;
                right_line = self.read_record(&mut right_lines, &map_right_headers_to_union);
            }
        }

        if self.merge_strategy != MergeStrategy::And {
            while let Some(left_record) = &left_line {
                if left_readed {
                    match &self.merge_strategy {
                        MergeStrategy::Or | MergeStrategy::AndNot => {
                            deduplicate_handler.add_row(left_record.clone(), Side::Left)?;
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
                            deduplicate_handler.add_row(right_record.clone(), Side::Right)?;
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

    fn get_left_reader(&self) -> Result<Reader<File>, csv::Error> {
        self.build_reader(&self.left_file_path)
    }
    fn get_right_reader(&self) -> Result<Reader<File>, csv::Error> {
        self.build_reader(&self.right_file_path)
    }

    fn build_reader(&self, path: &String) -> Result<Reader<File>, csv::Error> {
        ReaderBuilder::new().delimiter(b'\t').from_path(path)
    }

    fn get_headers(&self, reader: &mut Reader<File>) -> Result<Vec<Option<String>>, csv::Error> {
        let record: Vec<Option<String>> = reader
            .headers()?
            .iter()
            .map(|s| self.format_header(s.to_string()))
            .collect();
        Ok(record)
    }

    fn format_header(&self, header: String) -> Option<String> {
        match self.output_header_callback.clone() {
            Some(cb) => {
                let res = cb.call_async::<Option<String>>(header);
                let res = executor::block_on(res);
                res.unwrap()
            }
            None => Some(header),
        }
    }

    fn get_output_headers<'a>(
        &self,
        left_headers: &'a Vec<Option<String>>,
        right_headers: &'a Vec<Option<String>>
    ) -> Vec<String> {
        let mut result: Vec<String> = vec![];

        for header in left_headers {
            if let Some(header) = header {
                result.push(header.to_string());
            }
        }

        for header in right_headers {
            if let Some(header) = header {
                result.push(header.to_string());
            }
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
        file_headers: &Vec<Option<String>>
    ) -> HashMap<usize, Option<usize>> {
        output_headers
            .iter()
            .enumerate()
            .map(|(x, header)| (x, self.get_header_pos(file_headers, header)))
            .collect()
    }

    fn get_header_pos(&self, headers: &Vec<Option<String>>, name: &String) -> Option<usize> {
        headers.iter().position(|col| &col.as_ref().unwrap_or(&"".to_string()) == &name)
    }

    fn get_formatted_header_position(&self, headers: &Vec<String>, name: &String) -> Option<usize> {
        headers
            .iter()
            .position(|col| col == &self.format_header(name.to_string()).unwrap_or("".to_string()))
    }

    fn read_record(
        &self,
        iter: &mut ByteRecordsIntoIter<File>,
        mapping: &HashMap<usize, Option<usize>>
    ) -> Option<ByteRecord> {
        if let Some(Ok(record)) = iter.next() {
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