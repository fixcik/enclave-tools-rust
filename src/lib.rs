#![deny(clippy::all)]

use crate::csv::AsyncMergeTask;
use napi::bindgen_prelude::*;
use napi_derive::*;

extern crate napi_derive;

pub mod csv;

#[napi]
#[derive(PartialEq)]
pub enum MergeStrategy {
    Or,
    And,
    AndNot,
}

// #[derive(Clone, Copy)]
#[napi]
pub enum DeduplicateStrategy {
    KeepAll,
    KeepFirst,
    RemoveSimilar,
    Reduce,
    CrossJoin,
    CrossJoinAndRemoveSimilar,
}

#[napi(object)]
pub struct MergeOptions {
    pub output: String,
    pub merge_strategy: MergeStrategy,
    pub deduplicate_strategy: DeduplicateStrategy,
    pub left_key: String,
    pub right_key: String,
    pub is_number_key: Option<bool>,
}

#[napi(ts_return_type = "Promise<void>")]
pub fn merge(
    left_path: String,
    right_path: String,
    options: MergeOptions
) -> AsyncTask<AsyncMergeTask> {
    AsyncTask::new(AsyncMergeTask { left_path, right_path, options })
}