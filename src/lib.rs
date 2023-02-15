#![deny(clippy::all)]

use crate::csv::merge::Merger;

#[macro_use]
extern crate napi_derive;

pub mod csv;

#[napi]
pub fn sum() {
    let merger = Merger::new(
        "./__test__/fixtures/list1-sorted.csv".to_string(),
        "./__test__/fixtures/list2-sorted.csv".to_string(),
        // "./1.sorted.tsv".to_string(),
        // "./2.sorted.tsv".to_string(),
        csv::merge::MergeStrategy::And,
        "key".to_string(),
        "key".to_string(),
        true,
        "result.tsv".to_string()
    );

    merger.handle();
}