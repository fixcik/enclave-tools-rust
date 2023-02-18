use crate::{ MergeStrategy, DeduplicateStrategy };
use napi::{
    Task,
    Env,
    Result,
    Error,
    Status,
    bindgen_prelude::Undefined,
    threadsafe_function::{ ErrorStrategy, ThreadsafeFunction },
};

use self::merge::Merger;

pub mod deduplicate;
pub mod merge;

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

    fn compute(&mut self) -> Result<()> {
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

        merger.handle().map_err(|err| Error::new(Status::GenericFailure, err.to_string()))?;
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<Undefined> {
        Ok(())
    }
}