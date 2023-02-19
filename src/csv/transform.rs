use std::{ error::Error };

use csv::{ ReaderBuilder, WriterBuilder, ByteRecord };
use futures::executor;
use napi_derive::napi;
use napi::{
    bindgen_prelude::{ ToNapiValue },
    threadsafe_function::{ ThreadsafeFunction, ErrorStrategy, ThreadSafeCallContext },
    JsFunction,
    Env,
    JsObject,
};

#[derive(Debug)]
struct TransformError {
    message: String,
}

impl Error for TransformError {}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

fn create_transform_error(message: String) -> Result<(), Box<dyn std::error::Error>> {
    Err(
        Box::new(TransformError {
            message,
        })
    )
}

#[derive(Debug, PartialEq)]
#[napi]
pub enum FieldType {
    Number,
    String,
}

#[derive(Debug, PartialEq)]
#[napi]
pub enum Comparison {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug)]
pub struct Filter {
    index: Option<usize>,
    field: String,
    field_type: FieldType,
    comparison: Comparison,
    value: String,
}

impl Clone for Filter {
    fn clone(&self) -> Self {
        Self {
            index: None,
            field: self.field.clone(),
            field_type: self.field_type.clone(),
            comparison: self.comparison.clone(),
            value: self.value.clone(),
        }
    }
}

impl Filter {
    pub fn create(
        field: String,
        value: String,
        field_type: Option<FieldType>,
        comparison: Option<Comparison>
    ) -> Self {
        Self {
            index: None,
            value,
            field,
            field_type: field_type.unwrap_or(FieldType::String),
            comparison: comparison.unwrap_or(Comparison::Eq),
        }
    }
}

#[napi(js_name = "Filter")]
pub struct JsFilter {
    inner: Filter,
}

#[napi]
impl JsFilter {
    #[napi(constructor)]
    pub fn constructor(
        field: String,
        value: String,
        field_type: Option<FieldType>,
        comparison: Option<Comparison>
    ) -> Self {
        Self {
            inner: Filter::create(field, value, field_type, comparison),
        }
    }
}

pub struct Transform {
    path: String,
    delimiter: u8,
    filters: Vec<Filter>,
    columns_transform: Option<ThreadsafeFunction<String, ErrorStrategy::Fatal>>,
}

impl Transform {
    pub fn new(
        path: String
        // columns_transform: Option<fn(String) -> Option<String>>
    ) -> Self {
        Self {
            path,
            delimiter: b'\t',
            filters: vec![],
            columns_transform: None,
        }
    }

    pub fn with_delimiter(&mut self, delimiter: u8) {
        self.delimiter = delimiter;
    }

    pub fn set_columns_transform(
        &mut self,
        func: ThreadsafeFunction<String, ErrorStrategy::Fatal>
    ) {
        self.columns_transform = Some(func);
    }

    pub fn add_filter(&mut self, filter: &Filter) {
        self.filters.push(filter.clone());
    }

    pub fn save_to(&mut self, output: String) -> Result<(), Box<dyn Error>> {
        let mut reader = ReaderBuilder::new().delimiter(self.delimiter).from_path(&self.path)?;
        let orig_headers = reader
            .headers()?
            .iter()
            .map(|x| x.to_owned())
            .collect::<Vec<String>>();

        let mut headers: Vec<(usize, Option<String>)> = orig_headers
            .iter()
            .enumerate()
            .map(|(i, h)| (i, Some(h.to_string())))
            .collect();

        self.parse_filters(&orig_headers)?;

        if let Some(_) = &self.columns_transform {
            headers = orig_headers
                .iter()
                .enumerate()
                .map(|(i, header)| (i, self.format_header(header.to_string())))
                .collect();
        }

        let mut writer = WriterBuilder::new().delimiter(self.delimiter).from_path(output)?;

        let write_headers: Vec<String> = headers
            .iter()
            .filter(|x| x.1.is_some())
            .map(|x| x.clone().1.unwrap().to_string())
            .collect();

        writer.write_record(write_headers)?;

        for res in reader.byte_records() {
            let mut record = res?;
            if self.test_record(&record) {
                if self.columns_transform.is_some() && headers.iter().any(|(_, h)| h.is_none()) {
                    record = ByteRecord::from_iter(
                        record
                            .into_iter()
                            .enumerate()
                            .filter(|(index, _)| {
                                let r = headers.iter().find(|(i, _)| index == i);
                                r.is_some() && r.unwrap().1.is_some()
                            })
                            .map(|(_, head)| head)
                    );
                }
                writer.write_byte_record(&record)?;
            }
        }

        writer.flush()?;

        Ok(())
    }

    fn format_header(&self, header: String) -> Option<String> {
        match self.columns_transform.clone() {
            Some(cb) => {
                let res = cb.call_async::<Option<String>>(header);
                let res = executor::block_on(res);
                res.unwrap()
            }
            None => Some(header),
        }
    }

    fn parse_filters(&mut self, headers: &Vec<String>) -> Result<(), Box<dyn Error>> {
        self.filters
            .iter_mut()
            .map(
                |filter| -> Result<(), Box<dyn Error>> {
                    let index = headers.iter().position(|header| header.to_owned() == filter.field);
                    if index.is_none() {
                        return create_transform_error(
                            format!("Not found filter field {}", filter.field)
                        );
                    }
                    filter.index = index;
                    Ok(())
                }
            )
            .find(|x| x.is_err())
            .unwrap_or(Ok(()))
    }
    fn test_record(&self, record: &ByteRecord) -> bool {
        self.filters.iter().all(|filter| -> bool {
            if let Some(index) = filter.index {
                let record_value_raw = std::str::from_utf8(record.get(index).unwrap()).unwrap();

                return match filter.field_type {
                    FieldType::Number => {
                        let filter_value = filter.value.parse::<i64>().unwrap();
                        let parsed_value = record_value_raw.parse::<i64>().unwrap();
                        let cmp = parsed_value.cmp(&filter_value);

                        match filter.comparison {
                            Comparison::Eq => cmp.is_eq(),
                            Comparison::Lt => cmp.is_lt(),
                            Comparison::Le => cmp.is_le(),
                            Comparison::Gt => cmp.is_gt(),
                            Comparison::Ge => cmp.is_ge(),
                        }
                    }
                    FieldType::String => {
                        let filter_value = filter.value.to_owned();
                        let cmp = record_value_raw.to_string().cmp(&filter_value);

                        match filter.comparison {
                            Comparison::Eq => cmp.is_eq(),
                            Comparison::Lt => cmp.is_lt(),
                            Comparison::Le => cmp.is_le(),
                            Comparison::Gt => cmp.is_gt(),
                            Comparison::Ge => cmp.is_ge(),
                        }
                    }
                };
            }
            true
        })
    }
}

#[napi(js_name = "Transform")]
pub struct JsTransform {
    inner: Transform,
}

#[napi]
impl JsTransform {
    #[napi(constructor)]
    pub fn constructor(path: String) -> Self {
        Self {
            inner: Transform::new(path),
        }
    }
    #[napi]
    pub fn with_delimiter(&mut self, delimiter: u8) {
        self.inner.with_delimiter(delimiter)
    }

    #[napi]
    pub fn add_filter(&mut self, filter: &JsFilter) {
        self.inner.add_filter(&filter.inner)
    }

    #[napi]
    pub fn set_columns_transform(&mut self, column_transform: JsFunction) {
        let column_transform_ts: ThreadsafeFunction<String, ErrorStrategy::Fatal> = column_transform
            .create_threadsafe_function(0, |ctx: ThreadSafeCallContext<String>| {
                ctx.env.create_string(ctx.value.as_str()).map(|col| vec![col])
            })
            .unwrap();

        self.inner.set_columns_transform(column_transform_ts)
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn save_csv(&'static mut self, env: Env, path: String) -> Result<JsObject, napi::Error> {
        let res = env.execute_tokio_future(
            async move {
                self.inner.save_to(path).unwrap();
                Ok(())
            },
            |_env, _| Ok(())
        );

        res
    }
}

// pub struct AsyncTransformTask {
//     path: String,
//     output: String,
//     delimiter: u8,
//     filters: Vec<Filter>,
//     columns_transform: Option<ThreadsafeFunction<String, ErrorStrategy::Fatal>>,
// }

// impl Task for AsyncTransformTask {
//     type Output = Undefined;
//     type JsValue = ();

//     fn compute(&mut self) -> napi::Result<()> {
//         let mut transform = Transform::new(
//             self.path.to_owned(),
//             self.delimiter,
//             self.filters,
//             None
//         );

//         transform
//             .save_to(self.output.to_owned())
//             .map_err(|err| napi::Error::new(napi::Status::GenericFailure, err.to_string()))?;
//         Ok(())
//     }

//     fn resolve(&mut self, _env: napi::Env, _output: ()) -> napi::Result<Undefined> {
//         Ok(())
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    static FILE_FIXTURE: &str = "./__test__/fixtures/list1-sorted.csv";

    #[test]
    fn create_filter() {
        let filter = Filter::create(String::from("id"), String::from("42"), None, None);
        assert_eq!(filter.field, "id");
        assert_eq!(filter.value, "42");
        assert_eq!(filter.field_type, FieldType::String);
        assert_eq!(filter.comparison, Comparison::Eq);
    }

    #[test]
    fn test_transform_no_filters() {
        let mut transform = Transform::new(String::from(FILE_FIXTURE));

        let output_file = "./output_no_filters.csv";
        assert!(transform.save_to(output_file.to_string()).is_ok());
        fs::remove_file(&output_file).unwrap();
    }

    #[test]
    fn test_transform_with_filters() {
        let mut transform = Transform::new(String::from(FILE_FIXTURE));

        transform.add_filter(
            &Filter::create(
                String::from("key"),
                String::from("42"),
                Some(FieldType::Number),
                Some(Comparison::Ge)
            )
        );

        transform.add_filter(
            &Filter::create(
                String::from("feature2_left"),
                String::from("50"),
                Some(FieldType::Number),
                Some(Comparison::Ge)
            )
        );
        transform.add_filter(
            &Filter::create(
                String::from("feature_left"),
                String::from("7000"),
                Some(FieldType::String),
                Some(Comparison::Eq)
            )
        );

        let output_file = "./output_with_filters.csv";
        let res = transform.save_to(output_file.to_string());

        assert!(res.is_ok());

        let mut reader = ReaderBuilder::new().delimiter(b'\t').from_path(&output_file).unwrap();
        let headers = reader.headers().unwrap().iter().collect::<Vec<&str>>();

        assert_eq!(headers, ["key", "feature_left", "feature2_left"]);

        let records: Vec<Vec<String>> = reader
            .records()
            .map(|rec| rec.unwrap())
            .map(|rec|
                rec
                    .iter()
                    .map(|x| x.to_owned())
                    .collect::<Vec<String>>()
            )
            .collect();

        assert_eq!(records, [["300", "7000", "100"]]);
        fs::remove_file(&output_file).unwrap();
    }

    // #[test]
    // fn test_transform_with_columns_transform() {
    //     let columns_transform = |s: String| {
    //         match s.as_str() {
    //             "key" => Some(String::from("id")),
    //             "feature2_left" => Some(String::from("feature_changed_left")),
    //             _ => None,
    //         }
    //     };

    //     let mut transform = Transform::new(String::from(FILE_FIXTURE));

    //     transform.set_columns_transform(columns_transform);

    //     let output_file = String::from("./output_column_check.csv");
    //     assert!(transform.save_to(output_file.to_string()).is_ok());

    //     let mut reader = ReaderBuilder::new().delimiter(b'\t').from_path(&output_file).unwrap();
    //     let headers = reader.headers().unwrap().iter().collect::<Vec<&str>>();

    //     assert_eq!(headers, vec!["id", "feature_changed_left"]);
    //     fs::remove_file(&output_file).unwrap();
    // }
}