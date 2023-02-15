use std::fs::File;

use csv::{ ByteRecord, Writer };

pub enum DeduplicateStrategy {
    KeepAll,
    FirstOnly,
    RemoveSimilar,
    Reduce,
    CrossJoin,
}

pub trait StrategyHandler {
    fn add_row(&mut self, row: ByteRecord) -> Result<(), csv::Error>;
    fn flush(&mut self) -> Result<(), csv::Error>;
}

struct KeepAllStrategyHandler {
    writer: Writer<File>,
}

impl StrategyHandler for KeepAllStrategyHandler {
    fn add_row(&mut self, row: ByteRecord) -> Result<(), csv::Error> {
        self.writer.write_record(&row)
    }
    fn flush(&mut self) -> Result<(), csv::Error> {
        Ok(())
    }
}

pub struct FirstOnlyStrategyHandler {
    writer: Writer<File>,
    last_record: Option<ByteRecord>,
    key_index: usize,
    duplicates_counter: u32,
}

impl FirstOnlyStrategyHandler {
    pub fn build(writer: Writer<File>, key_index: usize) -> Self {
        FirstOnlyStrategyHandler {
            writer,
            last_record: None,
            key_index,
            duplicates_counter: 0,
        }
    }
}

impl StrategyHandler for FirstOnlyStrategyHandler {
    fn add_row(&mut self, row: ByteRecord) -> Result<(), csv::Error> {
        let eq = match &self.last_record {
            Some(lr) => lr.get(self.key_index).unwrap() == row.get(self.key_index).unwrap(),
            None => false,
        };

        if self.duplicates_counter == 0 {
            match &self.last_record {
                Some(lr) => self.writer.write_byte_record(lr)?,
                None => (),
            }
        }

        self.duplicates_counter = match eq {
            true => self.duplicates_counter + 1,
            false => 0,
        };

        self.last_record = Some(row.clone());

        Ok(())
    }

    fn flush(&mut self) -> Result<(), csv::Error> {
        if self.duplicates_counter == 0 {
            if let Some(lr) = &self.last_record {
                self.writer.write_byte_record(lr)?;
            }
        }
        Ok(())
    }
}
pub struct ReduceStrategyHandler {
    writer: Writer<File>,
    key_index: usize,
    group: Option<Vec<ByteRecord>>,
}

fn to_number(x: &[u8]) -> i64 {
    let string = String::from_utf8(x.to_vec()).expect(format!("Parse string: {:?}", x).as_str());

    string.parse::<i64>().expect(format!("Parse number: {}", string).as_str())
}

impl ReduceStrategyHandler {
    pub fn build(writer: Writer<File>, key_index: usize) -> Self {
        ReduceStrategyHandler {
            writer,
            key_index,
            group: None,
        }
    }

    fn flush_group(&mut self) -> Result<(), csv::Error> {
        if let Some(group) = self.group.take() {
            let mut reduced: Vec<&[u8]> = group[0].into_iter().collect();
            for record in group.iter().skip(1) {
                let key = record.get(self.key_index).unwrap();
                let reduced_key = *reduced.get(self.key_index).unwrap();
                if key != reduced_key {
                    panic!("Invalid data: keys in group do not match");
                }
                for (i, field) in record.iter().enumerate() {
                    if i == self.key_index {
                        continue;
                    }

                    if !field.is_empty() {
                        reduced[i] = field;
                    }
                }
            }

            self.writer.write_byte_record(&ByteRecord::from_iter(reduced))?;
        }
        Ok(())
    }
}

impl StrategyHandler for ReduceStrategyHandler {
    fn add_row(&mut self, row: ByteRecord) -> Result<(), csv::Error> {
        let key = row.get(self.key_index).unwrap();
        if let Some(group) = &mut self.group {
            let group_key = group[0].get(self.key_index).unwrap();
            if key == group_key {
                group.push(row);
                return Ok(());
            }
            self.flush_group()?;
        }
        self.group = Some(vec![row]);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), csv::Error> {
        self.flush_group()
    }
}