/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M2: streaming RFC4180-ish CSV parser and schema inference.
 *
 * Streaming strategy:
 *   - bytes are pulled from a fixed 64 KiB read buffer (no mmap, no
 *     read-the-whole-file),
 *   - infer_schema() parses every record but throws it away immediately,
 *   - next_batch() keeps at most `capacity` rows alive.
 * A single field is allowed to be arbitrarily large; only that field is
 * accumulated in memory while it is being parsed.
 */

#include "ob_csv_reader.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <unordered_map>
#include <utility>

namespace oceanbase
{
namespace storage
{
namespace
{

constexpr size_t kReadBufferSize = 64 * 1024;

/**
 * Byte-level CSV record parser.
 *
 * Supports quoted fields, embedded newlines (LF / CRLF, normalised to LF
 * inside quoted fields), doubled quotes, an optional separate escape
 * character and LF / CRLF / CR record terminators.  A UTF-8 BOM is skipped by
 * open() before the parser starts.
 */
class CsvRecordParser
{
public:
  CsvRecordParser(FILE *fp, const ObCsvOptions &options)
      : fp_(fp), options_(options), buffer_(kReadBufferSize)
  {
  }

  ~CsvRecordParser() = default;

  CsvRecordParser(const CsvRecordParser &) = delete;
  CsvRecordParser &operator=(const CsvRecordParser &) = delete;

  int reset(const int64_t offset)
  {
    if (fp_ == nullptr) {
      return -1;
    }
    if (std::fseek(fp_, static_cast<long>(offset), SEEK_SET) != 0) {
      return -1;
    }
    pos_ = 0;
    len_ = 0;
    consumed_ = offset;
    eof_ = false;
    failed_ = false;
    return 0;
  }

  int64_t consumed() const
  {
    return consumed_;
  }

  bool failed() const
  {
    return failed_;
  }

  /**
   * Parse one record.
   * @return 1 when a record was produced, 0 on clean EOF, -1 on I/O error.
   */
  int next_record(std::vector<std::string> &fields)
  {
    fields.clear();

    for (;;) {
      std::string field;
      bool in_quotes = false;
      bool field_quoted = false;
      bool record_started = false;

      for (;;) {
        const int byte = peek_byte();
        if (byte < 0) {
          if (failed_) {
            return -1;
          }
          if (!record_started && !field_quoted && fields.empty() && field.empty()) {
            return 0;
          }
          fields.push_back(finalize_field(field, field_quoted));
          return 1;
        }

        const char c = static_cast<char>(byte);
        if (in_quotes) {
          take_byte();
          record_started = true;
          if (c == options_.quote) {
            if (options_.escape == options_.quote) {
              // RFC4180 escaping: "" inside a quoted field is a single ".
              if (peek_byte() == static_cast<int>(options_.quote)) {
                take_byte();
                field.push_back(options_.quote);
              } else {
                in_quotes = false;
              }
            } else {
              in_quotes = false;
            }
          } else if (options_.escape != options_.quote && c == options_.escape) {
            // Backslash-style escaping: the next character is literal.
            const int next = peek_byte();
            if (next >= 0) {
              take_byte();
              field.push_back(static_cast<char>(next));
            } else {
              field.push_back(c);
            }
          } else if (c == '\r') {
            // Normalise CRLF / CR line breaks inside quoted fields to LF so
            // that a CRLF file produces the same value as an LF file.
            if (peek_byte() == '\n') {
              take_byte();
            }
            field.push_back('\n');
          } else if (c == '\n') {
            field.push_back('\n');
          } else {
            field.push_back(c);
          }
          continue;
        }

        if (c == options_.delimiter) {
          take_byte();
          record_started = true;
          fields.push_back(finalize_field(field, field_quoted));
          field.clear();
          field_quoted = false;
          continue;
        }

        if (c == '\n' || c == '\r') {
          take_byte();
          if (c == '\r' && peek_byte() == '\n') {
            take_byte();
          }
          if (!record_started && !field_quoted && fields.empty() && field.empty()) {
            // Completely blank line: ignore it and look at the next line.
            continue;
          }
          fields.push_back(finalize_field(field, field_quoted));
          return 1;
        }

        if (c == options_.quote && field.empty() && !field_quoted) {
          take_byte();
          in_quotes = true;
          field_quoted = true;
          record_started = true;
          continue;
        }

        take_byte();
        field.push_back(c);
        record_started = true;
      }
    }
  }

private:
  int peek_byte()
  {
    if (pos_ >= len_) {
      if (eof_) {
        return -1;
      }
      len_ = std::fread(buffer_.data(), 1, buffer_.size(), fp_);
      pos_ = 0;
      if (len_ == 0) {
        eof_ = true;
        if (std::ferror(fp_) != 0) {
          failed_ = true;
        }
        return -1;
      }
    }
    return static_cast<unsigned char>(buffer_[pos_]);
  }

  int take_byte()
  {
    const int byte = peek_byte();
    if (byte >= 0) {
      ++pos_;
      ++consumed_;
    }
    return byte;
  }

  std::string finalize_field(const std::string &field, const bool quoted) const
  {
    if (quoted || !options_.trim) {
      return field;
    }
    size_t begin = 0;
    size_t end = field.size();
    while (begin < end && (field[begin] == ' ' || field[begin] == '\t')) {
      ++begin;
    }
    while (end > begin && (field[end - 1] == ' ' || field[end - 1] == '\t')) {
      --end;
    }
    return field.substr(begin, end - begin);
  }

  FILE *fp_ = nullptr;
  const ObCsvOptions &options_;
  std::vector<char> buffer_;
  size_t pos_ = 0;
  size_t len_ = 0;
  int64_t consumed_ = 0;
  bool eof_ = false;
  bool failed_ = false;
};

/** Append __2, __3, ... to duplicate header names. */
void build_header_names(std::vector<std::string> &names)
{
  std::unordered_map<std::string, int> occurrences;
  for (size_t i = 0; i < names.size(); ++i) {
    if (names[i].empty()) {
      names[i] = "column" + std::to_string(i);
    }
    const auto it = occurrences.find(names[i]);
    if (it == occurrences.end()) {
      occurrences.emplace(names[i], 1);
    } else {
      ++it->second;
      names[i] = it->first + "__" + std::to_string(it->second);
    }
  }
}

} // namespace

struct ObCSVReader::Impl
{
  ~Impl()
  {
    close();
  }

  void close()
  {
    parser.reset();
    if (fp != nullptr) {
      std::fclose(fp);
      fp = nullptr;
    }
    header_names.clear();
    columns.clear();
    data_offset = 0;
    opened = false;
    eof = false;
    schema_inferred = false;
    scanned_rows = 0;
  }

  bool is_null_value(const std::string &value) const
  {
    return value.empty() ||
           (options.nullstr.has_value() && value == *options.nullstr);
  }

  int reset_to_data()
  {
    if (!opened || !parser) {
      return -1;
    }
    if (parser->reset(data_offset) != 0) {
      return -1;
    }
    eof = false;
    return 0;
  }

  size_t expected_columns() const
  {
    if (schema_inferred) {
      return columns.size();
    }
    if (options.header) {
      return header_names.size();
    }
    return 0;
  }

  FILE *fp = nullptr;
  ObCsvOptions options;
  std::unique_ptr<CsvRecordParser> parser;
  std::vector<std::string> header_names;
  std::vector<Column> columns;
  int64_t data_offset = 0;
  bool opened = false;
  bool eof = false;
  bool schema_inferred = false;
  int64_t scanned_rows = 0;
};

ObCSVReader::ObCSVReader() : impl_(new Impl())
{
}

ObCSVReader::~ObCSVReader() = default;

int ObCSVReader::open(const std::string &path, const ObCsvOptions &options)
{
  if (!impl_) {
    impl_.reset(new Impl());
  }
  impl_->close();

  FILE *fp = std::fopen(path.c_str(), "rb");
  if (fp == nullptr) {
    return -1;
  }
  impl_->fp = fp;
  impl_->options = options;

  // Skip a UTF-8 BOM: it belongs to the encoding, not to the first column
  // name.  data_offset is computed after this point.
  int64_t start = 0;
  unsigned char bom[3] = {0, 0, 0};
  const size_t bom_read = std::fread(bom, 1, sizeof(bom), fp);
  if (bom_read == sizeof(bom) && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF) {
    start = 3;
  }
  std::clearerr(fp);

  impl_->parser.reset(new CsvRecordParser(fp, impl_->options));
  if (impl_->parser->reset(start) != 0) {
    impl_->close();
    return -1;
  }

  impl_->data_offset = start;
  if (options.header) {
    std::vector<std::string> header;
    const int ret = impl_->parser->next_record(header);
    if (ret < 0) {
      impl_->close();
      return -1;
    }
    if (ret == 1) {
      impl_->header_names = std::move(header);
      impl_->data_offset = impl_->parser->consumed();
    }
  }

  impl_->opened = true;
  return 0;
}

void ObCSVReader::close()
{
  if (impl_) {
    impl_->close();
  }
}

bool ObCSVReader::is_open() const
{
  return impl_ && impl_->opened;
}

int ObCSVReader::infer_schema()
{
  if (!impl_ || !impl_->opened || !impl_->parser) {
    return -1;
  }
  Impl &state = *impl_;
  if (state.options.sample_size != -1) {
    // M2 deliberately supports only a full-file scan.
    return -1;
  }
  if (state.reset_to_data() != 0) {
    return -1;
  }

  state.columns.clear();
  state.scanned_rows = 0;
  state.schema_inferred = false;

  std::vector<std::string> names;
  std::vector<ObFileSqlType> types;
  std::vector<bool> nullable;
  bool have_columns = false;

  if (state.options.header) {
    names = state.header_names;
    if (!names.empty()) {
      build_header_names(names);
    }
    types.assign(names.size(), ObFileSqlType::NULLTYPE);
    nullable.assign(names.size(), false);
    have_columns = true;
  }

  std::vector<std::string> fields;
  for (;;) {
    const int ret = state.parser->next_record(fields);
    if (ret < 0) {
      return -1;
    }
    if (ret == 0) {
      break;
    }

    if (!have_columns) {
      // header=false: the first data record fixes the column set.
      names.resize(fields.size());
      for (size_t i = 0; i < fields.size(); ++i) {
        names[i] = "column" + std::to_string(i);
      }
      types.assign(fields.size(), ObFileSqlType::NULLTYPE);
      nullable.assign(fields.size(), false);
      have_columns = true;
    }

    ++state.scanned_rows;
    for (size_t i = 0; i < types.size(); ++i) {
      if (i >= fields.size()) {
        // A short row: the missing trailing fields are NULL.
        nullable[i] = true;
        continue;
      }
      const std::string &value = fields[i];
      if (state.is_null_value(value)) {
        nullable[i] = true;
        continue;
      }
      const ObFileSqlType cell_type = ob_file_sql_classify(value);
      types[i] = ob_file_sql_widen(types[i], cell_type);
    }
  }

  for (size_t i = 0; i < names.size(); ++i) {
    Column column;
    column.name = names[i];
    column.type = ob_file_sql_finalize(types[i]);
    // With no data rows every column is (vacuously) nullable.
    column.nullable = nullable[i] || state.scanned_rows == 0;
    state.columns.push_back(std::move(column));
  }

  state.schema_inferred = true;
  if (state.reset_to_data() != 0) {
    return -1;
  }
  return 0;
}

int ObCSVReader::next_batch(const size_t capacity, std::vector<Row> &rows)
{
  rows.clear();
  if (!impl_ || !impl_->opened || !impl_->parser) {
    return -1;
  }
  if (capacity == 0 || impl_->eof) {
    return 0;
  }

  const size_t expected = impl_->expected_columns();
  std::vector<std::string> fields;
  rows.reserve(capacity);

  while (rows.size() < capacity) {
    const int ret = impl_->parser->next_record(fields);
    if (ret < 0) {
      return -1;
    }
    if (ret == 0) {
      impl_->eof = true;
      break;
    }

    Row row;
    if (expected > 0) {
      row.fields.resize(expected);
      const size_t copy_count = std::min(expected, fields.size());
      for (size_t i = 0; i < copy_count; ++i) {
        if (!impl_->is_null_value(fields[i])) {
          row.fields[i] = fields[i];
        }
      }
    } else {
      row.fields = fields;
      for (size_t i = 0; i < row.fields.size(); ++i) {
        if (impl_->is_null_value(row.fields[i])) {
          row.fields[i].clear();
        }
      }
    }
    rows.push_back(std::move(row));
  }

  return 0;
}

int ObCSVReader::rewind()
{
  if (!impl_ || !impl_->opened) {
    return -1;
  }
  return impl_->reset_to_data();
}

const std::vector<Column> &ObCSVReader::columns() const
{
  static const std::vector<Column> empty_columns;
  return impl_ ? impl_->columns : empty_columns;
}

const ObCsvOptions &ObCSVReader::options() const
{
  static const ObCsvOptions default_options;
  return impl_ ? impl_->options : default_options;
}

bool ObCSVReader::eof() const
{
  return impl_ && impl_->eof;
}

int64_t ObCSVReader::scanned_row_count() const
{
  return impl_ ? impl_->scanned_rows : 0;
}

} // namespace storage
} // namespace oceanbase
