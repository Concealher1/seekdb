/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M4: standalone newline-delimited JSON (JSONL / NDJSON) reader.
 *
 * The reader is independent from the SQL engine, exactly like the CSV reader:
 * it turns a JSONL file into column definitions plus batches of raw string
 * cells.  Every line must be a flat JSON object; nested objects and arrays are
 * rejected.  JSON null (and a key that is absent from a record) is modelled as
 * an empty std::string, which is how the file-as-database layer models NULLs.
 *
 * Type inference reuses the CSV rules: each scalar value is classified with
 * ob_file_sql_classify() and column types are widened with
 * ob_file_sql_widen().  The schema is the union of the object keys in
 * first-appearance order.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "ob_csv_reader.h"
#include "ob_file_sql_type.h"

namespace oceanbase
{
namespace storage
{

/** Options accepted by ObJsonReader::open(). */
struct ObJsonOptions
{
  // M4 supports only newline-delimited JSON: exactly one flat object per line.
  // The field exists so that a future single-document / array format can be
  // selected without changing the interface.
  bool newline_delimited = true;
};

/** Reason of the last failure, used by the resolver to build a clear error. */
enum class ObJsonReadError
{
  NONE = 0,
  OPEN,
  IO,
  PARSE,
  NESTED
};

/**
 * Streaming JSONL reader.
 *
 * Typical use:
 *
 *   ObJsonReader reader;
 *   reader.open(path);
 *   reader.infer_schema();               // full scan, stream back at line 0
 *   while (reader.next_batch(1024, rows) == 0 && !rows.empty()) { ... }
 */
class ObJsonReader
{
public:
  ObJsonReader();
  ~ObJsonReader();

  ObJsonReader(const ObJsonReader &) = delete;
  ObJsonReader &operator=(const ObJsonReader &) = delete;

  /** Open a file.  Returns 0 on success, -1 on error. */
  int open(const std::string &path, const ObJsonOptions &options = ObJsonOptions());

  void close();
  bool is_open() const;

  /**
   * Infer the schema by scanning the whole file: union of the keys in
   * first-appearance order, types via CSV classification + widening.  On
   * success the stream is positioned at the first record.
   * Returns 0 on success, -1 on error (see last_error()).
   */
  int infer_schema();

  /**
   * Read at most `capacity` records.  Returns 0 on success (an empty vector
   * means EOF), -1 on error.
   */
  int next_batch(size_t capacity, std::vector<Row> &rows);

  /** Reposition the stream at the first record. */
  int rewind();

  /** Column definitions; empty until infer_schema() succeeded. */
  const std::vector<Column> &columns() const;

  const ObJsonOptions &options() const;

  bool eof() const;

  /** Number of non-empty JSON records seen by the last infer_schema() call. */
  int64_t scanned_row_count() const;

  ObJsonReadError last_error() const;
  const std::string &last_error_message() const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace storage
} // namespace oceanbase
