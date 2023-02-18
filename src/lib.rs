#![deny(clippy::all)]

use crate::csv::merge::AsyncMergeTask;
use napi::{
    bindgen_prelude::*,
    threadsafe_function::{ ThreadSafeCallContext, ThreadsafeFunction, ErrorStrategy },
};
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

    #[napi(ts_type = "(columnName: string) => string | undefined")]
    pub output_header_callback: Option<JsFunction>,
}

#[napi(ts_return_type = "Promise<void>")]
pub fn merge(
    left_path: String,
    right_path: String,
    options: MergeOptions
) -> AsyncTask<AsyncMergeTask> {
    let output_header_callback: Option<ThreadsafeFunction<String, ErrorStrategy::Fatal>> = match
        options.output_header_callback
    {
        Some(cb) =>
            cb
                .create_threadsafe_function(0, |ctx: ThreadSafeCallContext<String>| {
                    ctx.env.create_string(ctx.value.as_str()).map(|col| vec![col])
                })
                .ok(),
        None => None,
    };

    AsyncTask::new(AsyncMergeTask {
        left_path,
        right_path,
        output: options.output,
        merge_strategy: options.merge_strategy,
        deduplicate_strategy: options.deduplicate_strategy,
        left_key: options.left_key,
        right_key: options.right_key,
        is_number_key: options.is_number_key,
        output_header_callback,
    })
}