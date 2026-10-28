/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M2: standalone streaming CSV reader.
 *
 * The reader is independent from the SQL engine: it only turns a file into
 * column definitions plus batches of raw string cells.  Values that are NULL
 * (empty field, or the configured null string) are represented by an empty
 * std::string, which is how the file-as-database layer models CSV NULLs.
 */

#ifndef OCEANBASE_STORAGE_FILE_SQL_OB_CSV_READER_H_
#define OCEANBASE_STORAGE_FILE_SQL_OB_CSV_READER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "ob_file_sql_type.h"

namespace oceanbase
{
namespace storage
{

/** Options accepted by ObCSVReader::open(). */
struct ObCsvOptions
{
  char delimiter = ',';
  char quote = '"';
  // '"' means RFC4180 doubled quotes ("" -> "); any other character escapes
  // the following character inside a quoted field.
  char escape = '"';
  bool header = true;
  // When set, a field whose text equals this value is treated as NULL (in
  // addition to an empty field).
  std::optional<std::string> nullstr;
  // Trim ASCII spaces / tabs around *unquoted* fields.  Quoted field content
  // is always preserved verbatim.
  bool trim = false;
  // Only full-file inference (sample_size == -1) is implemented in M2.
  int64_t sample_size = -1;
};

/** One CSV row.  An empty field is NULL. */
struct Row
{
  std::vector<std::string> fields;

  bool is_null(const size_t index) const
  {
    return index >= fields.size() || fields[index].empty();
  }

  const std::string &operator[](const size_t index) const
  {
    return fields[index];
  }

  std::string &operator[](const size_t index)
  {
    return fields[index];
  }

  size_t size() const
  {
    return fields.size();
  }
};

/**
 * Streaming CSV reader.
 *
 * Typical use:
 *
 *   ObCSVReader reader;
 *   reader.open(path, options);
 *   reader.infer_schema();               // full scan, stream positioned at row 0
 *   while (reader.next_batch(1024, rows) == 0 && !rows.empty()) { ... }
 *
 * The file is never loaded into memory as a whole: bytes are consumed through
 * a fixed size read buffer and next_batch() only materialises at most
 * `capacity` rows.
 */
class ObCSVReader
{
public:
  ObCSVReader();
  ~ObCSVReader();

  ObCSVReader(const ObCSVReader &) = delete;
  ObCSVReader &operator=(const ObCSVReader &) = delete;

  /**
   * Open a file and parse the header row (when options.header is true).
   * Returns 0 on success, -1 on error.
   */
  int open(const std::string &path, const ObCsvOptions &options = ObCsvOptions());

  void close();
  bool is_open() const;

  /**
   * Infer the schema by scanning the whole file (sample_size must be -1).
   * Only column definitions are retained; rows are discarded as they are
   * parsed.  On success the stream is positioned at the first data row.
   * Returns 0 on success, -1 on error.
   */
  int infer_schema();

  /**
   * Read at most `capacity` rows.  Returns 0 on success (an empty vector means
   * EOF), -1 on error.
   */
  int next_batch(size_t capacity, std::vector<Row> &rows);

  /** Reposition the stream at the first data row (after the header). */
  int rewind();

  /** Column definitions; empty until infer_schema() succeeded. */
  const std::vector<Column> &columns() const;

  const ObCsvOptions &options() const;

  bool eof() const;

  /** Number of data rows seen by the last infer_schema() call. */
  int64_t scanned_row_count() const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_FILE_SQL_OB_CSV_READER_H_
