/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M4: standalone Parquet reader for flattened (non-nested) files.
 *
 * The reader has no OceanBase dependency: it parses the Parquet footer with a
 * small Thrift compact-protocol reader, decodes the data pages (PLAIN and
 * dictionary encodings, definition levels for optional columns) and exposes
 * batches of raw string cells, exactly like the CSV/JSON readers.
 *
 * Supported subset:
 *   - flat schemas (no groups / repeated fields),
 *   - PLAIN, PLAIN_DICTIONARY and RLE_DICTIONARY value encodings,
 *   - UNCOMPRESSED and SNAPPY page compression,
 *   - data page V1 and V2,
 *   - multiple row groups,
 *   - optional columns (definition levels) and column projection.
 *
 * Nested schemas, unsupported encodings or codecs are rejected with a clear
 * error instead of being silently mis-decoded.
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

struct ObParquetOptions
{
  // Empty means "decode every column".  Otherwise only these columns are
  // decoded; the other columns keep their schema entry but stay NULL.
  std::vector<std::string> projected_columns;
};

enum class ObParquetReadError
{
  NONE = 0,
  OPEN,
  IO,
  FORMAT,
  UNSUPPORTED
};

class ObParquetReader
{
public:
  ObParquetReader();
  ~ObParquetReader();

  ObParquetReader(const ObParquetReader &) = delete;
  ObParquetReader &operator=(const ObParquetReader &) = delete;

  /** Open a file.  Returns 0 on success, -1 on error. */
  int open(const std::string &path, const ObParquetOptions &options = ObParquetOptions());

  void close();
  bool is_open() const;

  /**
   * Parse the footer, build the column definitions and decode the projected
   * columns into memory.  Rows are then served by next_batch()/rewind().
   * Returns 0 on success, -1 on error (see last_error()).
   */
  int infer_schema();

  /** Read at most `capacity` rows.  Returns 0 on success, -1 on error. */
  int next_batch(size_t capacity, std::vector<Row> &rows);

  /** Reposition the stream at the first row. */
  int rewind();

  const std::vector<Column> &columns() const;

  const ObParquetOptions &options() const;

  bool eof() const;

  /** Number of rows declared by the Parquet footer. */
  int64_t scanned_row_count() const;

  ObParquetReadError last_error() const;
  const std::string &last_error_message() const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace storage
} // namespace oceanbase
