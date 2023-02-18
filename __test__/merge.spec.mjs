import test from "ava";
import os from "os";
import fs from "fs/promises";
import path from "path";

import { DeduplicateStrategy, MergeStrategy, merge } from "../index.js";

async function getTempFilePath() {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "tmp-"));
  const tempFileName = "output.txt";
  return path.join(tempDir, tempFileName);
}

test(`merge: test output header callback`, async (t) => {
  const output = await getTempFilePath();
  await t.notThrowsAsync(
    merge(
      "./__test__/fixtures/list1-sorted.csv",
      "./__test__/fixtures/list2-sorted.csv",
      {
        mergeStrategy: MergeStrategy.And,
        deduplicateStrategy: DeduplicateStrategy.Reduce,
        leftKey: "key",
        rightKey: "key",
        isNumberKey: true,
        output,
        outputHeaderCallback: (header) =>
          header.includes("_left")
            ? undefined
            : header === "myfeature_right"
            ? "myfeature"
            : header,
      }
    )
  );
  t.snapshot(await fs.readFile(output, { encoding: "ascii" }));
});

for (let mergeStrategy in MergeStrategy) {
  for (let deduplicateStrategy in DeduplicateStrategy) {
    test(`merge: ${mergeStrategy} - ${deduplicateStrategy}`, async (t) => {
      const output = await getTempFilePath();
      await t.notThrowsAsync(
        merge(
          "./__test__/fixtures/list1-sorted.csv",
          "./__test__/fixtures/list2-sorted.csv",
          {
            mergeStrategy: MergeStrategy[mergeStrategy],
            deduplicateStrategy: DeduplicateStrategy[deduplicateStrategy],
            leftKey: "key",
            rightKey: "key",
            isNumberKey: true,
            output,
          }
        )
      );
      t.snapshot(await fs.readFile(output, { encoding: "ascii" }));
      await fs.rm(output);
    });
  }
}
