import test from "ava";
import os from "os";
import fs from "fs/promises";

import { DeduplicateStrategy, MergeStrategy, merge } from "../index.js";

for (let mergeStrategy in MergeStrategy) {
  for (let deduplicateStrategy in DeduplicateStrategy) {
    test(`merge: ${mergeStrategy} - ${deduplicateStrategy}`, async (t) => {
      await merge(
        "./__test__/fixtures/list1-sorted.csv",
        "./__test__/fixtures/list2-sorted.csv",
        {
          mergeStrategy: MergeStrategy[mergeStrategy],
          deduplicateStrategy: DeduplicateStrategy[deduplicateStrategy],
          leftKey: "key",
          rightKey: "key",
          output: "./result.tsv",
        }
      );
      t.snapshot(await fs.readFile("./result.tsv", { encoding: "ascii" }));
    });
  }
}
