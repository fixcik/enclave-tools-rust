use crate::MergeOptions;
use napi::{ Task, Env, Result, Error, Status, bindgen_prelude::Undefined };

use self::merge::Merger;

pub mod deduplicate;
pub mod merge;

pub struct AsyncMergeTask {
    pub left_path: String,
    pub right_path: String,
    pub options: MergeOptions,
}

impl Task for AsyncMergeTask {
    type Output = Undefined;
    type JsValue = ();

    fn compute(&mut self) -> Result<()> {
        let merger = Merger::create(
            self.left_path.to_owned(),
            self.right_path.to_owned(),
            self.options.merge_strategy,
            self.options.deduplicate_strategy,
            self.options.left_key.to_owned(),
            self.options.right_key.to_owned(),
            self.options.is_number_key.unwrap_or(false),
            self.options.output.to_owned()
        );

        merger.handle().map_err(|err| Error::new(Status::GenericFailure, err.to_string()))?;
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> Result<Undefined> {
        Ok(())
    }
}