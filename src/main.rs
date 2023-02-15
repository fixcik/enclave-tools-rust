mod csv;

use ::csv::ByteRecord;
use ::csv::{ WriterBuilder };
use crate::csv::deduplicate::StrategyHandler;

use crate::csv::deduplicate::ReduceStrategyHandler;

fn main() {
    let writer = WriterBuilder::new().delimiter(0x09).from_path("./1.tsv").expect("open file");

    let mut handler = ReduceStrategyHandler::build(writer, 0);

    handler.add_row(ByteRecord::from(vec!["0", "1", "3"])).expect("add row");
    handler.add_row(ByteRecord::from(vec!["1", "2", ""])).expect("add row");
    handler.add_row(ByteRecord::from(vec!["1", "3", "4"])).expect("add row");
    handler.add_row(ByteRecord::from(vec!["2", "3", "5"])).expect("add row");
    handler.add_row(ByteRecord::from(vec!["3", "", "4"])).expect("add row");
    handler.add_row(ByteRecord::from(vec!["3", "6", ""])).expect("add row");
    handler.add_row(ByteRecord::from(vec!["3", "", "5"])).expect("add row");
    handler.add_row(ByteRecord::from(vec!["4", "", "5"])).expect("add row");

    handler.flush();

    // let merger = Merger::new(
    //   "./__test__/fixtures/list1-sorted.csv".to_string(),
    //   "./__test__/fixtures/list2-sorted.csv".to_string(),
    //   // "./1.sorted.tsv".to_string(),
    //   // "./2.sorted.tsv".to_string(),
    //   csv::merge::MergeStrategy::Or,
    //   "key".to_string(),
    //   "key".to_string(),
    //   true,
    //   "result.tsv".to_string(),
    // );

    // merger.handle();
}