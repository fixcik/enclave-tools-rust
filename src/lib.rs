#![deny(clippy::all)]

use crate::csv::merge::Merger;

#[macro_use]
extern crate napi_derive;

pub mod csv;

#[napi]
pub fn sum() {
    let merger = Merger::create(
        "./__test__/fixtures/list1-sorted.csv".to_string(),
        "./__test__/fixtures/list2-sorted.csv".to_string(),
        csv::merge::MergeStrategy::And,
        csv::deduplicate::DeduplicateStrategy::CrossJoin,
        "key".to_string(),
        "key".to_string(),
        true,
        "result.tsv".to_string()
    );

    merger.handle();
}