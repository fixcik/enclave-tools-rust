use std::cmp::Ordering;
use std::collections::{ HashSet, HashMap, BTreeSet };
use std::ops::Deref;
use std::{ io::BufReader, fs::File, error::Error, mem };
use std::str;

use csv::{ Reader, ReaderBuilder, StringRecord, WriterBuilder, Writer, ByteRecord };

#[derive(PartialEq)]
pub enum MergeStrategy {
    Or,
    And,
    AndNot,
}

pub struct Merger {
    left_reader: Reader<File>,
    right_reader: Reader<File>,
    strategy: MergeStrategy,
    left_key: String,
    right_key: String,
    number_key: bool,
    output: String,
}

impl Merger {
    pub fn create(
        left_file_path: String,
        right_file_path: String,
        strategy: MergeStrategy,
        left_key: String,
        right_key: String,
        number_key: bool,
        output: String
    ) -> Merger {
        let left_reader = ReaderBuilder::new()
            .delimiter(b'\t')
            .from_path(left_file_path)
            .expect("Open file");
        let right_reader = ReaderBuilder::new()
            .delimiter(b'\t')
            .from_path(right_file_path)
            .expect("Open file");

        Merger {
            left_reader: left_reader,
            right_reader: right_reader,
            strategy,
            left_key,
            right_key,
            output,
            number_key,
        }
    }

    fn write_row(
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

    fn compare(is_number: bool, a: &[u8], b: &[u8]) -> Ordering {
        if is_number {
            String::from_utf8(a.to_vec())
                .unwrap()
                .parse::<i64>()
                .unwrap()
                .cmp(&String::from_utf8(b.to_vec()).unwrap().parse::<i64>().unwrap())
        } else {
            a.cmp(b)
        }
    }

    pub fn handle(mut self) {
        let mut wtr = WriterBuilder::new()
            .delimiter(b'\t')
            .from_path("./1.tsv")
            .expect("open output file");

        let left_headers: Vec<String> = self.left_reader
            .headers()
            .expect("Left headers")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let left_headers_set: BTreeSet<&String> = left_headers.iter().collect();
        let right_headers: Vec<String> = self.right_reader
            .headers()
            .expect("Right headers")
            .iter()
            .map(|s| s.to_string())
            .collect();
        let right_headers_set: BTreeSet<&String> = right_headers.iter().collect();

        println!("{:?}", left_headers_set);
        println!("{:?}", right_headers_set);

        let union_columns: Vec<&String> = left_headers_set
            .union(&right_headers_set)
            .map(|&h| h)
            .collect();

        let map_left_headers_to_union: HashMap<usize, Option<usize>> = union_columns
            .iter()
            .enumerate()
            .map(|(x, &uc)| (x, left_headers.iter().position(|col| col == uc)))
            .collect();

        let map_right_headers_to_union: HashMap<usize, Option<usize>> = union_columns
            .iter()
            .enumerate()
            .map(|(x, &uc)| (x, right_headers.iter().position(|col| col == uc)))
            .collect();

        wtr.write_record(&union_columns).expect("write output headers");

        println!("union columns: {:?}", union_columns);

        println!("left_headers: {:?}, map to union: {:?}", left_headers, map_left_headers_to_union);

        println!(
            "right_headers: {:?}, map to union: {:?}",
            right_headers,
            map_right_headers_to_union
        );

        let left_col_index = left_headers
            .iter()
            .position(|col| *col == self.left_key)
            .expect("Has column in left file");

        let right_col_index = right_headers
            .iter()
            .position(|col| *col == self.right_key)
            .expect("Has column in right file");

        println!("{:?} {:?}", left_col_index, right_col_index);

        let mut left_lines = self.left_reader.into_byte_records();
        let mut right_lines = self.right_reader.into_byte_records();

        let mut left_line = left_lines.next();
        let mut right_line = right_lines.next();

        let mut old_left_value: Option<Vec<u8>> = None;

        let mut left_new = true;
        let mut right_new = true;

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

            let cmp = Merger::compare(self.number_key, &left_value, &right_value);

            old_left_eq_right = match &old_left_value {
                Some(v) => v == right_value,
                _ => false,
            };
            need_read_left = old_left_eq_right && cmp != Ordering::Equal;

            need_left_push = match &self.strategy {
                MergeStrategy::And => left_new && cmp == Ordering::Equal,
                MergeStrategy::Or => left_new && !need_read_left,
                MergeStrategy::AndNot => left_new && cmp == Ordering::Less && !need_read_left,
            };
            if need_left_push {
                // println!("left: {:?}", left_record);
                Merger::write_row(&mut wtr, &map_left_headers_to_union, left_record);
                left_new = false;
            }
            let need_right_push = match self.strategy {
                MergeStrategy::Or => right_new,
                MergeStrategy::AndNot => false,
                MergeStrategy::And => right_new && (cmp == Ordering::Equal || old_left_eq_right),
            };
            if need_right_push {
                // println!("right: {:?} ", right_record);
                Merger::write_row(&mut wtr, &map_right_headers_to_union, right_record);
                right_new = false;
            }

            if (cmp == Ordering::Less || cmp == Ordering::Equal) && !need_read_left {
                left_new = true;
                // println!("read left");
                old_left_value = Some(left_value.to_owned());
                left_line = left_lines.next();
            } else {
                right_new = true;
                // println!("read right");
                right_line = right_lines.next();
            }
        }

        if self.strategy != MergeStrategy::And {
            while let Some(Ok(left_record)) = &left_line {
                if left_new {
                    match &self.strategy {
                        MergeStrategy::Or | MergeStrategy::AndNot => {
                            // println!("left: {:?}", left_record);
                            Merger::write_row(&mut wtr, &map_left_headers_to_union, left_record);
                        }
                        _ => (),
                    }
                }

                left_line = left_lines.next();
                left_new = true;
            }
        }

        if self.strategy != MergeStrategy::And {
            while let Some(Ok(right_record)) = &right_line {
                if right_new {
                    match &self.strategy {
                        MergeStrategy::Or => {
                            // println!("right: {:?}", right_record);
                            Merger::write_row(&mut wtr, &map_right_headers_to_union, right_record);
                        }
                        _ => (),
                    }
                }

                right_line = right_lines.next();
                right_new = true;
            }
        }
    }
}