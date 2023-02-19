use std::cmp::Ordering;
use std::collections::{ HashMap, HashSet };
use std::fs::File;
use std::vec;

use csv::{ ByteRecord, ByteRecordsIntoIter, Reader, ReaderBuilder, Writer, WriterBuilder };
use napi::Task;
use napi::bindgen_prelude::Undefined;
use napi::threadsafe_function::{ ThreadsafeFunction, ErrorStrategy };

use futures::executor;

use crate::{ MergeStrategy, DeduplicateStrategy };

use super::deduplicate::{ Side };
use super::utils::{ is_empty_file, create_empty_file };

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
        if is_empty_file(&self.left_file_path)? || is_empty_file(&self.right_file_path)? {
            create_empty_file(&self.output)?;
            return Ok(());
        }

        let mut left_reader = self.get_left_reader()?;
        let mut right_reader = self.get_right_reader()?;

        let (left_headers, left_key_index) = self.get_headers(&mut left_reader, &self.left_key)?;
        let (right_headers, right_key_index) = self.get_headers(
            &mut right_reader,
            &self.right_key
        )?;

        let output_headers = self.get_output_headers(&left_headers, &right_headers);

        let map_left_headers_to_union = self.map_file_headers_to_output(
            &output_headers,
            &left_headers
        );

        let map_right_headers_to_union = self.map_file_headers_to_output(
            &output_headers,
            &right_headers
        );

        let left_key_index = left_key_index.expect(
            format!("Has column {} in left file", self.left_key).as_str()
        );

        let right_key_index = right_key_index.expect(
            format!("Has column {} in right file", self.right_key).as_str()
        );

        let mut writer = self.get_writer();

        writer.write_record(&output_headers).expect("write output headers");

        let mut deduplicate_handler = DeduplicateStrategy::create(
            self.deduplicate_strategy,
            &mut writer
        );

        let mut left_lines = left_reader.into_byte_records();
        let mut right_lines = right_reader.into_byte_records();

        let mut left_line = self.read_record(
            &mut left_lines,
            &map_left_headers_to_union,
            Some(left_key_index)
        );
        let mut right_line = self.read_record(
            &mut right_lines,
            &map_right_headers_to_union,
            Some(right_key_index)
        );

        let mut old_left_value: Option<Vec<u8>> = None;

        let mut left_readed = true;
        let mut right_readed = true;

        let mut need_left_push;
        let mut need_right_push;
        let mut old_left_eq_right;
        let mut need_read_left;

        let mut counter = 0;
        while
            let (Some((left_record, Some(left_value))), Some((right_record, Some(right_value)))) = (
                &left_line,
                &right_line,
            )
        {
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
                deduplicate_handler.add_row(left_record.clone(), left_value.to_vec(), Side::Left)?;
                // self.write_row(&mut writer, &map_left_headers_to_union, left_record);
                left_readed = false;
            }
            if need_right_push {
                deduplicate_handler.add_row(
                    right_record.clone(),
                    right_value.to_vec(),
                    Side::Right
                )?;
                // self.write_row(&mut writer, &map_right_headers_to_union, right_record);
                right_readed = false;
            }

            if cmp.is_le() && !need_read_left {
                left_readed = true;
                old_left_value = Some(left_value.to_vec());
                left_line = self.read_record(
                    &mut left_lines,
                    &map_left_headers_to_union,
                    Some(left_key_index)
                );
            } else {
                right_readed = true;
                right_line = self.read_record(
                    &mut right_lines,
                    &map_right_headers_to_union,
                    Some(right_key_index)
                );
            }
        }

        if self.merge_strategy != MergeStrategy::And {
            while let Some((left_record, value)) = &left_line {
                if left_readed {
                    match &self.merge_strategy {
                        MergeStrategy::Or | MergeStrategy::AndNot => {
                            deduplicate_handler.add_row(
                                left_record.clone(),
                                value.as_ref().unwrap().to_vec(),
                                Side::Left
                            )?;
                        }
                        _ => (),
                    }
                }

                left_line = self.read_record(
                    &mut left_lines,
                    &map_left_headers_to_union,
                    Some(left_key_index)
                );
                left_readed = true;
            }
        }

        if self.merge_strategy != MergeStrategy::And {
            while let Some((right_record, value)) = &right_line {
                if right_readed {
                    match &self.merge_strategy {
                        MergeStrategy::Or => {
                            deduplicate_handler.add_row(
                                right_record.clone(),
                                value.as_ref().unwrap().to_vec(),
                                Side::Right
                            )?;
                        }
                        _ => (),
                    }
                }

                right_line = self.read_record(
                    &mut right_lines,
                    &map_right_headers_to_union,
                    Some(right_key_index)
                );
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

    fn get_headers(
        &self,
        reader: &mut Reader<File>,
        key: &String
    ) -> Result<(Vec<Option<String>>, Option<usize>), csv::Error> {
        let mut key_index = None;
        let record: Vec<Option<String>> = reader
            .headers()?
            .iter()
            .enumerate()
            .map(|(index, s)| {
                if s.to_string() == *key {
                    key_index = Some(index);
                }
                self.format_header(s.to_string())
            })
            .collect();
        Ok((record, key_index))
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

    fn read_record(
        &self,
        iter: &mut ByteRecordsIntoIter<File>,
        mapping: &HashMap<usize, Option<usize>>,
        key_index: Option<usize>
    ) -> Option<(ByteRecord, Option<Vec<u8>>)> {
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
            let new_record = ByteRecord::from_iter(&values);
            if let Some(key_index) = key_index {
                let key_value = record.get(key_index).unwrap().to_owned();
                return Some((new_record, Some(key_value.clone())));
            }
            return Some((new_record, None));
        }
        None
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if self.number_key { to_number(a).cmp(&to_number(b)) } else { a.cmp(b) }
    }
}

pub struct AsyncMergeTask {
    pub left_path: String,
    pub right_path: String,
    pub output: String,
    pub merge_strategy: MergeStrategy,
    pub deduplicate_strategy: DeduplicateStrategy,
    pub left_key: String,
    pub right_key: String,
    pub is_number_key: Option<bool>,
    pub output_header_callback: Option<ThreadsafeFunction<String, ErrorStrategy::Fatal>>,
}

impl Task for AsyncMergeTask {
    type Output = Undefined;
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<()> {
        let merger = Merger::create(
            self.left_path.to_owned(),
            self.right_path.to_owned(),
            self.merge_strategy,
            self.deduplicate_strategy,
            self.left_key.to_owned(),
            self.right_key.to_owned(),
            self.is_number_key.unwrap_or(false),
            self.output.to_owned(),
            self.output_header_callback.clone()
        );

        merger
            .handle()
            .map_err(|err| napi::Error::new(napi::Status::GenericFailure, err.to_string()))?;
        Ok(())
    }

    fn resolve(&mut self, _env: napi::Env, _output: ()) -> napi::Result<Undefined> {
        Ok(())
    }
}