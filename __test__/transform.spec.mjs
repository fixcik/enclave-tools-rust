import test from "ava";
import fs from "fs/promises";
import path from "path";
import os from "os";

import { Comparison, FieldType, Filter, Transform } from "../index.js";

async function getTempFilePath() {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "tmp-"));
  const tempFileName = "output.txt";
  return path.join(tempDir, tempFileName);
}

test("Test transform", async (t) => {
  const output = await getTempFilePath();
  const transform = new Transform("./__test__/fixtures/list1-sorted.csv");

  transform.setColumnsTransform((column) => {
    if (column === "feature_left") return;
    return `${column}_new`;
  });
  transform.addFilter(new Filter("key", "15", FieldType.Number, Comparison.Le));
  transform.addFilter(
    new Filter("feature2_left", "1", FieldType.Number, Comparison.Eq)
  );

  transform.appendLineNumber();

  await transform.saveCsv(output);

  t.snapshot(await fs.readFile(output, { encoding: "ascii" }));
  await fs.rm(output);
});
