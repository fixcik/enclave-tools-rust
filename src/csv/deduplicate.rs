use std::{ fs::File };

use csv::{ ByteRecord, Writer };

pub enum DeduplicateStrategy {
    KeepAll,
    FirstOnly,
    RemoveSimilar,
    Reduce,
    CrossJoin,
}

#[derive(PartialEq, Debug)]
pub enum Side {
    Left,
    Right,
}

pub trait StrategyHandler {
    fn add_row(&mut self, row: ByteRecord, side: Side) -> Result<(), csv::Error>;
    fn flush(&mut self) -> Result<(), csv::Error>;
}

struct KeepAllStrategyHandler {
    writer: Writer<File>,
}

impl StrategyHandler for KeepAllStrategyHandler {
    fn add_row(&mut self, row: ByteRecord, _side: Side) -> Result<(), csv::Error> {
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
    fn add_row(&mut self, row: ByteRecord, _side: Side) -> Result<(), csv::Error> {
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
    fn add_row(&mut self, row: ByteRecord, _side: Side) -> Result<(), csv::Error> {
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

pub struct CrossJoinStrategyHandler {
    writer: Writer<File>,
    last_row: Option<ByteRecord>,
    duplicates: Vec<(ByteRecord, Side)>,
    key_index: usize,
}

impl CrossJoinStrategyHandler {
    pub fn build(writer: Writer<File>, key_index: usize) -> Self {
        CrossJoinStrategyHandler {
            writer,
            last_row: None,
            duplicates: vec![],
            key_index,
        }
    }

    fn flush_duplicates(&mut self) -> Result<(), csv::Error> {
        let left_records: Vec<ByteRecord> = self.duplicates
            .iter()
            .filter(|x| x.1 == Side::Left)
            .map(|x| x.0.clone())
            .collect();

        let right_records: Vec<ByteRecord> = self.duplicates
            .iter()
            .filter(|x| x.1 == Side::Right)
            .map(|x| x.0.clone())
            .collect();

        if right_records.len() > 0 && left_records.len() > 0 {
            for left_record in left_records {
                for right_record in &right_records {
                    let mut computed: Vec<&[u8]> = left_record.iter().collect();
                    for (i, field) in right_record.iter().enumerate() {
                        if !field.is_empty() {
                            computed[i] = field;
                        }
                    }
                    self.writer.write_byte_record(&ByteRecord::from_iter(computed))?;
                }
            }
        } else if right_records.len() > 0 {
            for right_record in right_records {
                self.writer.write_byte_record(&right_record)?;
            }
        } else {
            for left_record in left_records {
                self.writer.write_byte_record(&left_record)?;
            }
        }

        self.duplicates = vec![];
        Ok(())
    }
}

impl StrategyHandler for CrossJoinStrategyHandler {
    fn add_row(&mut self, row: ByteRecord, side: Side) -> Result<(), csv::Error> {
        let is_equal;
        if let Some(last_row) = &self.last_row {
            is_equal = last_row.get(self.key_index).unwrap() == row.get(self.key_index).unwrap();
            if !is_equal && self.duplicates.len() > 0 {
                self.flush_duplicates()?;
            }
            if (is_equal && self.duplicates.len() > 0) || (!is_equal && self.duplicates.len() == 0) {
                self.duplicates.push((row.clone(), side));
            } else {
                self.flush_duplicates()?;
            }
        } else {
            self.duplicates.push((row.clone(), side));
        }
        self.last_row = Some(row);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), csv::Error> {
        self.flush_duplicates()?;
        Ok(())
    }
}

pub struct RemoveSimilarStrategyHandler {
    writer: Writer<File>,
    last_row: Option<ByteRecord>,
    duplicates: Vec<ByteRecord>,
    key_index: usize,
}

impl RemoveSimilarStrategyHandler {
    pub fn build(writer: Writer<File>, key_index: usize) -> Self {
        RemoveSimilarStrategyHandler {
            writer,
            last_row: None,
            duplicates: vec![],
            key_index,
        }
    }

    fn flush_duplicates(&mut self) -> Result<(), csv::Error> {
        self.duplicates.dedup();

        for record in &self.duplicates {
            self.writer.write_byte_record(record)?;
        }

        self.duplicates = vec![];
        Ok(())
    }
}

impl StrategyHandler for RemoveSimilarStrategyHandler {
    fn add_row(&mut self, row: ByteRecord, _side: Side) -> Result<(), csv::Error> {
        let is_equal;
        if let Some(last_row) = &self.last_row {
            is_equal = last_row.get(self.key_index).unwrap() == row.get(self.key_index).unwrap();
            if !is_equal && self.duplicates.len() > 0 {
                self.flush_duplicates()?;
            }
            if (is_equal && self.duplicates.len() > 0) || (!is_equal && self.duplicates.len() == 0) {
                self.duplicates.push(row.clone());
            } else {
                self.flush_duplicates()?;
            }
        } else {
            self.duplicates.push(row.clone());
        }
        self.last_row = Some(row);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), csv::Error> {
        self.flush_duplicates()?;
        Ok(())
    }
}