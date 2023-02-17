use std::{ fs::File };

use csv::{ ByteRecord, Writer };

use crate::DeduplicateStrategy;

pub enum DeduplicateStrategyHandler<'a> {
    KeepAll(KeepAllStrategyHandler<'a>),
    FirstOnly(KeepFirstStrategyHandler<'a>),
    RemoveSimilar(RemoveSimilarStrategyHandler<'a>),
    Reduce(ReduceStrategyHandler<'a>),
    CrossJoin(CrossJoinStrategyHandler<'a>),
    CrossJoinAndRemoveSimilar(CrossJoinStrategyHandler<'a>),
}

impl<'a> DeduplicateStrategyHandler<'a> {
    pub fn add_row(&mut self, row: ByteRecord, side: Side) -> Result<(), csv::Error> {
        match self {
            DeduplicateStrategyHandler::KeepAll(handler) => handler.add_row(row, side),
            DeduplicateStrategyHandler::FirstOnly(handler) => handler.add_row(row, side),
            DeduplicateStrategyHandler::RemoveSimilar(handler) => handler.add_row(row, side),
            DeduplicateStrategyHandler::Reduce(handler) => handler.add_row(row, side),
            DeduplicateStrategyHandler::CrossJoin(handler) => handler.add_row(row, side),
            DeduplicateStrategyHandler::CrossJoinAndRemoveSimilar(handler) =>
                handler.add_row(row, side),
        }
    }

    pub fn flush(&mut self) -> Result<(), csv::Error> {
        match self {
            DeduplicateStrategyHandler::KeepAll(handler) => handler.flush(),
            DeduplicateStrategyHandler::FirstOnly(handler) => handler.flush(),
            DeduplicateStrategyHandler::RemoveSimilar(handler) => handler.flush(),
            DeduplicateStrategyHandler::Reduce(handler) => handler.flush(),
            DeduplicateStrategyHandler::CrossJoin(handler) => handler.flush(),
            DeduplicateStrategyHandler::CrossJoinAndRemoveSimilar(handler) => handler.flush(),
        }
    }
}

impl DeduplicateStrategy {
    pub fn create<'a>(
        strategy: DeduplicateStrategy,
        writer: &'a mut Writer<File>,
        left_key_index: usize,
        right_key_index: usize
    ) -> DeduplicateStrategyHandler {
        match strategy {
            DeduplicateStrategy::KeepAll =>
                DeduplicateStrategyHandler::KeepAll(KeepAllStrategyHandler::build(writer)),
            DeduplicateStrategy::KeepFirst =>
                DeduplicateStrategyHandler::FirstOnly(
                    KeepFirstStrategyHandler::build(writer, left_key_index, right_key_index)
                ),
            DeduplicateStrategy::RemoveSimilar =>
                DeduplicateStrategyHandler::RemoveSimilar(
                    RemoveSimilarStrategyHandler::build(writer, left_key_index, right_key_index)
                ),
            DeduplicateStrategy::Reduce =>
                DeduplicateStrategyHandler::Reduce(
                    ReduceStrategyHandler::build(writer, left_key_index, right_key_index)
                ),
            DeduplicateStrategy::CrossJoin =>
                DeduplicateStrategyHandler::CrossJoin(
                    CrossJoinStrategyHandler::build(writer, left_key_index, right_key_index, false)
                ),
            DeduplicateStrategy::CrossJoinAndRemoveSimilar =>
                DeduplicateStrategyHandler::CrossJoin(
                    CrossJoinStrategyHandler::build(writer, left_key_index, right_key_index, true)
                ),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum Side {
    Left,
    Right,
}

pub trait StrategyHandler {
    fn add_row(&mut self, row: ByteRecord, side: Side) -> Result<(), csv::Error>;
    fn flush(&mut self) -> Result<(), csv::Error>;
}

pub struct KeepAllStrategyHandler<'a> {
    writer: &'a mut Writer<File>,
}

impl<'a> KeepAllStrategyHandler<'a> {
    pub fn build(writer: &'a mut Writer<File>) -> Self {
        KeepAllStrategyHandler {
            writer,
        }
    }
}

impl<'a> StrategyHandler for KeepAllStrategyHandler<'a> {
    fn add_row(&mut self, row: ByteRecord, _side: Side) -> Result<(), csv::Error> {
        self.writer.write_record(&row)
    }
    fn flush(&mut self) -> Result<(), csv::Error> {
        self.writer.flush()?;
        Ok(())
    }
}

pub struct KeepFirstStrategyHandler<'a> {
    writer: &'a mut Writer<File>,
    last_record: Option<(ByteRecord, Side)>,
    left_key_index: usize,
    right_key_index: usize,
    duplicates_counter: u32,
}

impl<'a> KeepFirstStrategyHandler<'a> {
    pub fn build(
        writer: &'a mut Writer<File>,
        left_key_index: usize,
        right_key_index: usize
    ) -> Self {
        KeepFirstStrategyHandler {
            writer,
            last_record: None,
            left_key_index,
            right_key_index,
            duplicates_counter: 0,
        }
    }
}

impl<'a> StrategyHandler for KeepFirstStrategyHandler<'a> {
    fn add_row(&mut self, row: ByteRecord, side: Side) -> Result<(), csv::Error> {
        let eq = match &self.last_record {
            Some((lr, lr_side)) =>
                lr
                    .get(
                        if lr_side.eq(&Side::Left) {
                            self.left_key_index
                        } else {
                            self.right_key_index
                        }
                    )
                    .unwrap() ==
                    row
                        .get(
                            if side.eq(&Side::Left) {
                                self.left_key_index
                            } else {
                                self.right_key_index
                            }
                        )
                        .unwrap(),
            None => false,
        };

        if self.duplicates_counter == 0 {
            match &self.last_record {
                Some((lr, _side)) => self.writer.write_byte_record(lr)?,
                None => (),
            }
        }

        self.duplicates_counter = match eq {
            true => self.duplicates_counter + 1,
            false => 0,
        };

        self.last_record = Some((row, side));

        Ok(())
    }

    fn flush(&mut self) -> Result<(), csv::Error> {
        if self.duplicates_counter == 0 {
            if let Some((lr, _side)) = &self.last_record {
                self.writer.write_byte_record(lr)?;
            }
        }
        self.writer.flush()?;
        Ok(())
    }
}
pub struct ReduceStrategyHandler<'a> {
    writer: &'a mut Writer<File>,
    left_key_index: usize,
    right_key_index: usize,
    group: Option<Vec<(ByteRecord, Side)>>,
}

impl<'a> ReduceStrategyHandler<'a> {
    pub fn build(
        writer: &'a mut Writer<File>,
        left_key_index: usize,
        right_key_index: usize
    ) -> Self {
        ReduceStrategyHandler {
            writer,
            left_key_index,
            right_key_index,
            group: None,
        }
    }

    fn flush_group(&mut self) -> Result<(), csv::Error> {
        if let Some(group) = self.group.take() {
            let mut reduced: Vec<&[u8]> = group[0].0.into_iter().collect();
            for (record, _record_side) in group.iter().skip(1) {
                for (i, field) in record.iter().enumerate() {
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

impl<'a> StrategyHandler for ReduceStrategyHandler<'a> {
    fn add_row(&mut self, row: ByteRecord, side: Side) -> Result<(), csv::Error> {
        let key = row
            .get(if side.eq(&Side::Left) { self.left_key_index } else { self.right_key_index })
            .unwrap();
        if let Some(group) = &mut self.group {
            let (record, record_size) = &group[0];
            let group_key = record
                .get(
                    if record_size.eq(&Side::Left) {
                        self.left_key_index
                    } else {
                        self.right_key_index
                    }
                )
                .unwrap();
            if key == group_key {
                group.push((row, side));
                return Ok(());
            }
            self.flush_group()?;
        }
        self.group = Some(vec![(row, side)]);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), csv::Error> {
        self.flush_group()?;
        self.writer.flush()?;
        Ok(())
    }
}

pub struct CrossJoinStrategyHandler<'a> {
    writer: &'a mut Writer<File>,
    last_row: Option<(ByteRecord, Side)>,
    duplicates: Vec<(ByteRecord, Side)>,
    left_key_index: usize,
    right_key_index: usize,
    remove_similar: bool,
}

impl<'a> CrossJoinStrategyHandler<'a> {
    pub fn build(
        writer: &'a mut Writer<File>,
        left_key_index: usize,
        right_key_index: usize,
        remove_similar: bool
    ) -> Self {
        CrossJoinStrategyHandler {
            writer,
            last_row: None,
            duplicates: vec![],
            left_key_index,
            right_key_index,
            remove_similar,
        }
    }

    fn flush_duplicates(&mut self) -> Result<(), csv::Error> {
        if self.remove_similar {
            self.duplicates.sort_by(|a, b| a.0.as_slice().cmp(&b.0.as_slice()));
            self.duplicates.dedup();
        }

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

impl<'a> StrategyHandler for CrossJoinStrategyHandler<'a> {
    fn add_row(&mut self, row: ByteRecord, side: Side) -> Result<(), csv::Error> {
        let is_equal;
        if let Some((last_row, last_row_side)) = &self.last_row {
            is_equal =
                last_row
                    .get(
                        if last_row_side.eq(&Side::Left) {
                            self.left_key_index
                        } else {
                            self.right_key_index
                        }
                    )
                    .unwrap() ==
                row
                    .get(
                        if side.eq(&Side::Left) {
                            self.left_key_index
                        } else {
                            self.right_key_index
                        }
                    )
                    .unwrap();
            if !is_equal && self.duplicates.len() > 0 {
                self.flush_duplicates()?;
            }
            if (is_equal && self.duplicates.len() > 0) || (!is_equal && self.duplicates.len() == 0) {
                self.duplicates.push((row.clone(), side.clone()));
            } else {
                self.flush_duplicates()?;
            }
        } else {
            self.duplicates.push((row.clone(), side.clone()));
        }
        self.last_row = Some((row, side));
        Ok(())
    }

    fn flush(&mut self) -> Result<(), csv::Error> {
        self.flush_duplicates()?;
        self.writer.flush()?;
        Ok(())
    }
}

pub struct RemoveSimilarStrategyHandler<'a> {
    writer: &'a mut Writer<File>,
    last_row: Option<(ByteRecord, Side)>,
    duplicates: Vec<ByteRecord>,
    left_key_index: usize,
    right_key_index: usize,
}

impl<'a> RemoveSimilarStrategyHandler<'a> {
    pub fn build(
        writer: &'a mut Writer<File>,
        left_key_index: usize,
        right_key_index: usize
    ) -> Self {
        RemoveSimilarStrategyHandler {
            writer,
            last_row: None,
            duplicates: vec![],
            left_key_index,
            right_key_index,
        }
    }

    fn flush_duplicates(&mut self) -> Result<(), csv::Error> {
        self.duplicates.sort_by(|a, b| a.as_slice().cmp(&b.as_slice()));
        self.duplicates.dedup();

        for record in &self.duplicates {
            self.writer.write_byte_record(record)?;
        }

        self.duplicates = vec![];
        Ok(())
    }
}

impl<'a> StrategyHandler for RemoveSimilarStrategyHandler<'a> {
    fn add_row(&mut self, row: ByteRecord, side: Side) -> Result<(), csv::Error> {
        let is_equal;
        if let Some((last_row, last_row_side)) = &self.last_row {
            is_equal =
                last_row
                    .get(
                        if last_row_side.eq(&Side::Left) {
                            self.left_key_index
                        } else {
                            self.right_key_index
                        }
                    )
                    .unwrap() ==
                row
                    .get(
                        if side.eq(&Side::Left) {
                            self.left_key_index
                        } else {
                            self.right_key_index
                        }
                    )
                    .unwrap();
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
        self.last_row = Some((row, side));
        Ok(())
    }

    fn flush(&mut self) -> Result<(), csv::Error> {
        self.flush_duplicates()?;
        self.writer.flush()?;
        Ok(())
    }
}