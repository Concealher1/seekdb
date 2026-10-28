/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M2 standalone validation program for the CSV reader.
 *
 * Build (from the repository root, WITHOUT the OceanBase build system):
 *
 *   g++ -std=c++17 -O2 -Wall -Wextra -o /tmp/csv_selftest \
 *       src/storage/file_sql/{all .cpp files}
 *   /tmp/csv_selftest
 */

#include <cstdio>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "ob_csv_reader.h"
#include "ob_file_sql_type.h"
#include "ob_json_reader.h"
#include "ob_parquet_reader.h"

using namespace oceanbase::storage;

namespace
{

int g_checks = 0;
int g_failures = 0;

class GroupGuard
{
public:
  explicit GroupGuard(const char *name)
      : name_(name), failures_before_(g_failures)
  {
    std::cout << "\n--- " << name_ << " ---\n";
  }

  ~GroupGuard()
  {
    if (g_failures == failures_before_) {
      std::cout << "PASS: " << name_ << "\n";
    } else {
      std::cout << "FAIL: " << name_ << "\n";
    }
  }

  GroupGuard(const GroupGuard &) = delete;
  GroupGuard &operator=(const GroupGuard &) = delete;

private:
  const char *name_;
  int failures_before_;
};

void check_true(const bool condition, const std::string &what)
{
  ++g_checks;
  if (!condition) {
    ++g_failures;
    std::cout << "FAIL: " << what << "\n";
  }
}

template <typename T>
void check_equal(const T &actual, const T &expected, const std::string &what)
{
  ++g_checks;
  if (!(actual == expected)) {
    ++g_failures;
    std::cout << "FAIL: " << what << " (expected=[" << expected
              << "], actual=[" << actual << "])\n";
  }
}

void check_equal_type(const ObFileSqlType actual, const ObFileSqlType expected,
                      const std::string &what)
{
  ++g_checks;
  if (actual != expected) {
    ++g_failures;
    std::cout << "FAIL: " << what << " (expected=" << ob_file_sql_type_name(expected)
              << ", actual=" << ob_file_sql_type_name(actual) << ")\n";
  }
}

struct ExpectedColumn
{
  const char *name;
  ObFileSqlType type;
  bool nullable;
};

void check_schema(const std::vector<Column> &columns,
                  const std::vector<ExpectedColumn> &expected,
                  const std::string &label)
{
  check_equal(columns.size(), expected.size(), label + " column count");
  const size_t common = columns.size() < expected.size() ? columns.size() : expected.size();
  for (size_t i = 0; i < common; ++i) {
    const std::string prefix = label + " column " + std::to_string(i);
    check_equal(columns[i].name, std::string(expected[i].name), prefix + " name");
    check_equal_type(columns[i].type, expected[i].type, prefix + " type");
    check_equal(columns[i].nullable, expected[i].nullable, prefix + " nullability");
  }
}

void print_schema(const std::vector<Column> &columns)
{
  std::cout << "  inferred schema:\n";
  for (size_t i = 0; i < columns.size(); ++i) {
    std::cout << "    " << columns[i].name << " "
              << ob_file_sql_type_name(columns[i].type) << " "
              << (columns[i].nullable ? "YES" : "NO") << "\n";
  }
}

bool read_all(ObCSVReader &reader, const size_t batch_size, std::vector<Row> &rows)
{
  rows.clear();
  for (;;) {
    std::vector<Row> batch;
    if (reader.next_batch(batch_size, batch) != 0) {
      return false;
    }
    if (batch.empty()) {
      break;
    }
    if (batch.size() > batch_size) {
      return false;
    }
    for (size_t i = 0; i < batch.size(); ++i) {
      rows.push_back(std::move(batch[i]));
    }
  }
  return true;
}

bool read_all(ObJsonReader &reader, const size_t batch_size, std::vector<Row> &rows)
{
  rows.clear();
  for (;;) {
    std::vector<Row> batch;
    if (reader.next_batch(batch_size, batch) != 0) {
      return false;
    }
    if (batch.empty()) {
      break;
    }
    if (batch.size() > batch_size) {
      return false;
    }
    for (size_t i = 0; i < batch.size(); ++i) {
      rows.push_back(std::move(batch[i]));
    }
  }
  return true;
}

bool read_all(ObParquetReader &reader, const size_t batch_size, std::vector<Row> &rows)
{
  rows.clear();
  for (;;) {
    std::vector<Row> batch;
    if (reader.next_batch(batch_size, batch) != 0) {
      return false;
    }
    if (batch.empty()) {
      break;
    }
    if (batch.size() > batch_size) {
      return false;
    }
    for (size_t i = 0; i < batch.size(); ++i) {
      rows.push_back(std::move(batch[i]));
    }
  }
  return true;
}

std::string read_binary_file(const std::string &path)
{
  std::ifstream in(path.c_str(), std::ios::binary);
  if (!in.is_open()) {
    return std::string();
  }
  std::ostringstream buffer;
  buffer << in.rdbuf();
  return buffer.str();
}

std::string decode_base64(const std::string &input)
{
  static const char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  int values[256];
  for (int i = 0; i < 256; ++i) {
    values[i] = -1;
  }
  for (int i = 0; i < 64; ++i) {
    values[static_cast<unsigned char>(alphabet[i])] = i;
  }
  std::string output;
  uint32_t accumulator = 0;
  int bits = 0;
  for (size_t i = 0; i < input.size(); ++i) {
    const unsigned char c = static_cast<unsigned char>(input[i]);
    if (c == '=') {
      break;
    }
    if (values[c] < 0) {
      continue;
    }
    accumulator = (accumulator << 6) | static_cast<uint32_t>(values[c]);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      output.push_back(static_cast<char>((accumulator >> bits) & 0xFF));
    }
  }
  return output;
}

std::string parquet_fixture_path(const std::string &basic_path)
{
  const std::string directory = basic_path.substr(0, basic_path.find_last_of('/') + 1);
  const std::string plain = directory + "simple.parquet";
  std::ifstream probe(plain.c_str(), std::ios::binary);
  if (probe.is_open()) {
    return plain;
  }
  const std::string encoded = read_binary_file(directory + "simple.parquet.b64");
  const std::string decoded = decode_base64(encoded);
  const std::string output = "/tmp/ob_file_sql_m4_simple.parquet";
  std::ofstream out(output.c_str(), std::ios::binary | std::ios::trunc);
  out.write(decoded.data(), static_cast<std::streamsize>(decoded.size()));
  out.close();
  return output;
}

std::string write_temp(const std::string &name, const std::string &content)
{
  const std::string path = "/tmp/ob_file_sql_m2_" + name;
  std::ofstream out(path.c_str(), std::ios::binary | std::ios::trunc);
  out.write(content.data(), static_cast<std::streamsize>(content.size()));
  out.close();
  return path;
}

void remove_temp(const std::string &path)
{
  std::remove(path.c_str());
}

void test_basic_schema(const std::string &path)
{
  GroupGuard guard("basic.csv schema inference (8 columns, DuckDB DESCRIBE shape)");

  ObCSVReader reader;
  check_equal(reader.open(path), 0, "open basic.csv");
  if (!reader.is_open()) {
    return;
  }
  check_equal(reader.infer_schema(), 0, "infer_schema basic.csv");
  check_equal(reader.scanned_row_count(), static_cast<int64_t>(3),
              "infer_schema scans the whole file");

  const std::vector<Column> &columns = reader.columns();
  print_schema(columns);

  const std::vector<ExpectedColumn> expected = {
      {"id", ObFileSqlType::BIGINT, false},
      {"name", ObFileSqlType::VARCHAR, false},
      {"score", ObFileSqlType::DOUBLE, false},
      {"active", ObFileSqlType::BOOLEAN, false},
      {"day", ObFileSqlType::DATE, true},
      {"created", ObFileSqlType::DATETIME, true},
      {"note", ObFileSqlType::VARCHAR, false},
      {"id__2", ObFileSqlType::BIGINT, false},
  };
  check_schema(columns, expected, "basic.csv");
  check_equal(columns[7].name, std::string("id__2"), "duplicate header renamed id__2");
}

void test_basic_rows(const std::string &path)
{
  GroupGuard guard("basic.csv streaming row decoding");

  ObCSVReader reader;
  check_equal(reader.open(path), 0, "open basic.csv");
  if (!reader.is_open()) {
    return;
  }
  check_equal(reader.infer_schema(), 0, "infer_schema basic.csv");

  std::vector<Row> rows;
  check_true(read_all(reader, 2, rows), "read basic.csv in batches of 2");
  check_equal(rows.size(), static_cast<size_t>(3), "basic.csv data row count");
  if (rows.size() != 3) {
    return;
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    check_equal(rows[i].size(), static_cast<size_t>(8),
                "row " + std::to_string(i + 1) + " field count");
  }

  check_equal(rows[0][0], std::string("1"), "row 1 id");
  check_equal(rows[0][1], std::string("Alice"), "row 1 name");
  check_equal(rows[0][2], std::string("98.5"), "row 1 score");
  check_equal(rows[0][3], std::string("true"), "row 1 active");
  check_equal(rows[0][4], std::string("2026-08-01"), "row 1 day");
  check_equal(rows[0][5], std::string("2026-08-01 10:11:12"), "row 1 created");
  check_equal(rows[0][6], std::string("hello,\nworld"),
              "row 1 note keeps the embedded newline");
  check_true(rows[0][6].find('\r') == std::string::npos,
             "row 1 note contains no CR");
  check_equal(rows[0][7], std::string("101"), "row 1 id__2");

  check_equal(rows[1][1], std::string("Bob"), "row 2 name");
  check_equal(rows[1][2], std::string("87"), "row 2 score");
  check_equal(rows[1][3], std::string("false"), "row 2 active");
  check_true(rows[1].is_null(4), "row 2 day is NULL");
  check_true(rows[1].is_null(5), "row 2 created is NULL");
  check_equal(rows[1][6], std::string("plain"), "row 2 note");
  check_equal(rows[1][7], std::string("102"), "row 2 id__2");

  check_equal(rows[2][1], std::string("Chen"), "row 3 name");
  check_equal(rows[2][2], std::string("90.25"), "row 3 score");
  check_true(rows[2].is_null(4) == false, "row 3 day is not NULL");
  check_equal(rows[2][4], std::string("2026-08-03"), "row 3 day value");
  check_equal(rows[2][6], std::string("say \"hi\""),
              "row 3 note keeps escaped quotes");
  check_equal(rows[2][7], std::string("103"), "row 3 id__2");

  ObFileSqlDateTime created;
  check_true(ob_file_sql_parse_datetime(rows[2][5], created),
             "row 3 created is a valid DATETIME");
  check_equal(created.date.year, static_cast<int32_t>(2026), "row 3 created year");
  check_equal(created.date.month, static_cast<int32_t>(8), "row 3 created month");
  check_equal(created.date.day, static_cast<int32_t>(3), "row 3 created day");
  check_equal(created.hour, static_cast<int32_t>(12), "row 3 created hour");
  check_equal(created.minute, static_cast<int32_t>(13), "row 3 created minute");
  check_equal(created.second, static_cast<int32_t>(14), "row 3 created second");
  check_equal(rows[2][5], std::string("2026-08-03T12:13:14"),
              "row 3 created keeps ISO T separator in the raw value");
  check_equal_type(ob_file_sql_classify(rows[2][5]), ObFileSqlType::DATETIME,
                   "row 3 created classifies as DATETIME");
  check_equal_type(ob_file_sql_classify(rows[0][5]), ObFileSqlType::DATETIME,
                   "row 1 created classifies as DATETIME");

  // EOF / rewind behaviour.
  std::vector<Row> extra;
  check_equal(reader.next_batch(1, extra), 0, "next_batch at EOF returns success");
  check_true(extra.empty(), "next_batch at EOF returns an empty batch");
  check_true(reader.eof(), "eof flag is set after the last row");
  check_equal(reader.rewind(), 0, "rewind succeeds");
  check_true(!reader.eof(), "rewind clears the eof flag");
  check_equal(reader.next_batch(1, extra), 0, "next_batch after rewind");
  check_equal(extra.size(), static_cast<size_t>(1), "one row after rewind");
  if (!extra.empty()) {
    check_equal(extra[0][0], std::string("1"), "rewind restarts at the first data row");
  }
}

void test_crlf_and_quotes()
{
  GroupGuard guard("CRLF input, quoted comma and doubled quotes");

  const std::string content =
      "id,name,note\r\n"
      "1,Alice,\"hello,\r\nworld\"\r\n"
      "2,Bob,\"a,b\"\r\n"
      "3,Chen,\"say \"\"hi\"\"\"\r\n";
  const std::string path = write_temp("crlf.csv", content);

  ObCSVReader reader;
  check_equal(reader.open(path), 0, "open CRLF file");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer CRLF schema");
    check_schema(reader.columns(),
                 {{"id", ObFileSqlType::BIGINT, false},
                  {"name", ObFileSqlType::VARCHAR, false},
                  {"note", ObFileSqlType::VARCHAR, false}},
                 "crlf.csv");

    std::vector<Row> rows;
    check_true(read_all(reader, 1, rows), "read CRLF file one row at a time");
    check_equal(rows.size(), static_cast<size_t>(3), "CRLF row count");
    if (rows.size() == 3) {
      check_equal(rows[0][2], std::string("hello,\nworld"),
                  "CRLF embedded newline normalised to LF");
      check_true(rows[0][2].find('\r') == std::string::npos,
                 "CRLF embedded newline contains no CR");
      check_equal(rows[1][2], std::string("a,b"),
                  "quoted field containing a comma");
      check_equal(rows[2][2], std::string("say \"hi\""),
                  "doubled quotes decode to one quote");
    }
  }

  reader.close();
  remove_temp(path);
}

void test_empty_column()
{
  GroupGuard guard("entirely empty column infers VARCHAR + nullable");

  const std::string path = write_temp(
      "empty_column.csv", "a,b,c\n1,,x\n2,,y\n");
  ObCSVReader reader;
  check_equal(reader.open(path), 0, "open empty-column file");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer empty-column schema");
    check_schema(reader.columns(),
                 {{"a", ObFileSqlType::BIGINT, false},
                  {"b", ObFileSqlType::VARCHAR, true},
                  {"c", ObFileSqlType::VARCHAR, false}},
                 "empty_column.csv");

    std::vector<Row> rows;
    check_true(read_all(reader, 10, rows), "read empty-column rows");
    check_equal(rows.size(), static_cast<size_t>(2), "empty-column row count");
    if (rows.size() == 2) {
      check_true(rows[0].is_null(1), "row 1 empty column is NULL");
      check_true(rows[1].is_null(1), "row 2 empty column is NULL");
      check_equal(rows[0][0], std::string("1"), "empty-column row 1 a");
      check_equal(rows[1][0], std::string("2"), "empty-column row 2 a");
    }
  }

  reader.close();
  remove_temp(path);
}

void test_widening()
{
  GroupGuard guard("BIGINT widens to DOUBLE on a decimal row");

  const std::string decimal_path = write_temp(
      "widen.csv", "n\n1\n2.5\n");
  ObCSVReader reader;
  check_equal(reader.open(decimal_path), 0, "open widening file");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer widening schema");
    check_schema(reader.columns(), {{"n", ObFileSqlType::DOUBLE, false}},
                 "widen.csv");
  }
  reader.close();
  remove_temp(decimal_path);

  const std::string integer_path = write_temp(
      "integer.csv", "n\n1\n2\n");
  check_equal(reader.open(integer_path), 0, "open integer file");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer integer schema");
    check_schema(reader.columns(), {{"n", ObFileSqlType::BIGINT, false}},
                 "integer.csv");
  }
  reader.close();
  remove_temp(integer_path);
}

void test_headerless()
{
  GroupGuard guard("header=false uses column0, column1, ...");

  const std::string path = write_temp("headerless.csv", "1,2\n3,4\n");
  ObCsvOptions options;
  options.header = false;

  ObCSVReader reader;
  check_equal(reader.open(path, options), 0, "open headerless file");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer headerless schema");
    check_schema(reader.columns(),
                 {{"column0", ObFileSqlType::BIGINT, false},
                  {"column1", ObFileSqlType::BIGINT, false}},
                 "headerless.csv");

    std::vector<Row> rows;
    check_true(read_all(reader, 10, rows), "read headerless rows");
    check_equal(rows.size(), static_cast<size_t>(2), "headerless row count");
    if (rows.size() == 2) {
      check_equal(rows[0][0], std::string("1"), "headerless row 1 column0");
      check_equal(rows[0][1], std::string("2"), "headerless row 1 column1");
      check_equal(rows[1][0], std::string("3"), "headerless row 2 column0");
    }
  }

  reader.close();
  remove_temp(path);
}

void test_headers_options_and_bom(const std::string &basic_path)
{
  GroupGuard guard("sample_size guard, triple duplicate headers, BOM, custom escape");

  ObCsvOptions unsupported;
  unsupported.sample_size = 10;
  ObCSVReader unsupported_reader;
  check_equal(unsupported_reader.open(basic_path, unsupported), 0,
              "open stores the sample_size option");
  check_equal(unsupported_reader.infer_schema(), -1,
              "infer_schema rejects sample_size != -1");
  unsupported_reader.close();

  const std::string duplicate_path = write_temp(
      "triple_duplicate.csv", "id,id,id\n1,2,3\n");
  ObCSVReader duplicate_reader;
  check_equal(duplicate_reader.open(duplicate_path), 0,
              "open triple-duplicate file");
  if (duplicate_reader.is_open()) {
    check_equal(duplicate_reader.infer_schema(), 0,
                "infer triple-duplicate schema");
    check_schema(duplicate_reader.columns(),
                 {{"id", ObFileSqlType::BIGINT, false},
                  {"id__2", ObFileSqlType::BIGINT, false},
                  {"id__3", ObFileSqlType::BIGINT, false}},
                 "triple_duplicate.csv");
  }
  duplicate_reader.close();
  remove_temp(duplicate_path);

  const std::string bom_path = write_temp(
      "bom.csv", "\xEF\xBB\xBF" "id,name\n1,Alice\n");
  ObCSVReader bom_reader;
  check_equal(bom_reader.open(bom_path), 0, "open UTF-8 BOM file");
  if (bom_reader.is_open()) {
    check_equal(bom_reader.infer_schema(), 0, "infer BOM schema");
    check_schema(bom_reader.columns(),
                 {{"id", ObFileSqlType::BIGINT, false},
                  {"name", ObFileSqlType::VARCHAR, false}},
                 "bom.csv");
    check_equal(bom_reader.columns()[0].name, std::string("id"),
                "BOM is not part of the first column name");
    std::vector<Row> rows;
    check_true(read_all(bom_reader, 10, rows), "read BOM file");
    if (rows.size() == 1) {
      check_equal(rows[0][0], std::string("1"), "BOM file first value");
    }
  }
  bom_reader.close();
  remove_temp(bom_path);

  const std::string escape_path = write_temp(
      "custom_escape.csv", "v\n\"a\\\"b\"\n");
  ObCsvOptions escape_options;
  escape_options.escape = '\\';
  ObCSVReader escape_reader;
  check_equal(escape_reader.open(escape_path, escape_options), 0,
              "open custom-escape file");
  if (escape_reader.is_open()) {
    check_equal(escape_reader.infer_schema(), 0, "infer custom-escape schema");
    std::vector<Row> rows;
    check_true(read_all(escape_reader, 10, rows), "read custom-escape rows");
    if (rows.size() == 1) {
      check_equal(rows[0][0], std::string("a\"b"),
                  "backslash escape decodes to a quote");
    }
  }
  escape_reader.close();
  remove_temp(escape_path);

  const std::string no_newline_path = write_temp(
      "no_newline.csv", "a,b\n1,2");
  ObCSVReader no_newline_reader;
  check_equal(no_newline_reader.open(no_newline_path), 0,
              "open file without a trailing newline");
  if (no_newline_reader.is_open()) {
    check_equal(no_newline_reader.infer_schema(), 0,
                "infer file without a trailing newline");
    std::vector<Row> rows;
    check_true(read_all(no_newline_reader, 10, rows),
               "read file without a trailing newline");
    check_equal(rows.size(), static_cast<size_t>(1),
                "no extra row at EOF without a trailing newline");
  }
  no_newline_reader.close();
  remove_temp(no_newline_path);
}

void test_nullstr_and_trim()
{
  GroupGuard guard("nullstr and trim options");

  const std::string null_path = write_temp("nullstr.csv", "v\nNULL\n1\n");
  ObCsvOptions null_options;
  null_options.nullstr = std::string("NULL");
  ObCSVReader null_reader;
  check_equal(null_reader.open(null_path, null_options), 0, "open nullstr file");
  if (null_reader.is_open()) {
    check_equal(null_reader.infer_schema(), 0, "infer nullstr schema");
    check_schema(null_reader.columns(), {{"v", ObFileSqlType::BIGINT, true}},
                 "nullstr.csv");
    std::vector<Row> rows;
    check_true(read_all(null_reader, 10, rows), "read nullstr rows");
    check_equal(rows.size(), static_cast<size_t>(2), "nullstr row count");
    if (rows.size() == 2) {
      check_true(rows[0].is_null(0), "configured null string becomes NULL");
      check_equal(rows[1][0], std::string("1"), "non-null value after nullstr");
    }
  }
  null_reader.close();
  remove_temp(null_path);

  const std::string trim_path = write_temp(
      "trim.csv", "v,w\n 1 ,\" x \"\n");
  ObCsvOptions trim_options;
  trim_options.trim = true;
  ObCSVReader trim_reader;
  check_equal(trim_reader.open(trim_path, trim_options), 0, "open trim file");
  if (trim_reader.is_open()) {
    check_equal(trim_reader.infer_schema(), 0, "infer trim schema");
    check_schema(trim_reader.columns(),
                 {{"v", ObFileSqlType::BIGINT, false},
                  {"w", ObFileSqlType::VARCHAR, false}},
                 "trim.csv");
    std::vector<Row> rows;
    check_true(read_all(trim_reader, 10, rows), "read trim rows");
    if (rows.size() == 1) {
      check_equal(rows[0][0], std::string("1"), "unquoted field is trimmed");
      check_equal(rows[0][1], std::string(" x "),
                  "quoted field content is not trimmed");
    }
  }
  trim_reader.close();
  remove_temp(trim_path);
}

void test_classification_edges()
{
  GroupGuard guard("valid/invalid dates, case-insensitive booleans, fractional seconds");

  const std::string invalid_date_path = write_temp(
      "invalid_date.csv", "d\n2026-02-29\n");
  ObCSVReader invalid_reader;
  check_equal(invalid_reader.open(invalid_date_path), 0, "open invalid-date file");
  if (invalid_reader.is_open()) {
    check_equal(invalid_reader.infer_schema(), 0, "infer invalid-date schema");
    check_schema(invalid_reader.columns(), {{"d", ObFileSqlType::VARCHAR, false}},
                 "invalid_date.csv");
  }
  invalid_reader.close();
  remove_temp(invalid_date_path);

  const std::string valid_date_path = write_temp(
      "valid_date.csv", "d\n2026-02-28\n");
  ObCSVReader valid_reader;
  check_equal(valid_reader.open(valid_date_path), 0, "open valid-date file");
  if (valid_reader.is_open()) {
    check_equal(valid_reader.infer_schema(), 0, "infer valid-date schema");
    check_schema(valid_reader.columns(), {{"d", ObFileSqlType::DATE, false}},
                 "valid_date.csv");
  }
  valid_reader.close();
  remove_temp(valid_date_path);

  const std::string boolean_path = write_temp(
      "boolean.csv", "b\nTRUE\nFalse\n");
  ObCSVReader boolean_reader;
  check_equal(boolean_reader.open(boolean_path), 0, "open boolean file");
  if (boolean_reader.is_open()) {
    check_equal(boolean_reader.infer_schema(), 0, "infer boolean schema");
    check_schema(boolean_reader.columns(), {{"b", ObFileSqlType::BOOLEAN, false}},
                 "boolean.csv");
  }
  boolean_reader.close();
  remove_temp(boolean_path);

  const std::string datetime_path = write_temp(
      "fractional.csv", "t\n2026-08-03T12:13:14.123456\n");
  ObCSVReader datetime_reader;
  check_equal(datetime_reader.open(datetime_path), 0, "open fractional file");
  if (datetime_reader.is_open()) {
    check_equal(datetime_reader.infer_schema(), 0, "infer fractional schema");
    check_schema(datetime_reader.columns(), {{"t", ObFileSqlType::DATETIME, false}},
                 "fractional.csv");
    std::vector<Row> rows;
    check_true(read_all(datetime_reader, 10, rows), "read fractional rows");
    if (rows.size() == 1) {
      ObFileSqlDateTime value;
      check_true(ob_file_sql_parse_datetime(rows[0][0], value),
                 "fractional DATETIME parses");
      check_equal(value.microsecond, static_cast<int32_t>(123456),
                  "fractional DATETIME microseconds");
    }
  }
  datetime_reader.close();
  remove_temp(datetime_path);
}

void test_streaming_large()
{
  GroupGuard guard("streaming batches on a 20k row file");

  std::ostringstream csv;
  csv << "id,text\n";
  const int row_count = 20000;
  for (int i = 0; i < row_count; ++i) {
    csv << i << ",row" << i << "\n";
  }
  const std::string path = write_temp("large.csv", csv.str());

  ObCSVReader reader;
  check_equal(reader.open(path), 0, "open large file");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer large-file schema");
    check_equal(reader.scanned_row_count(), static_cast<int64_t>(row_count),
                "large-file full scan row count");
    check_schema(reader.columns(),
                 {{"id", ObFileSqlType::BIGINT, false},
                  {"text", ObFileSqlType::VARCHAR, false}},
                 "large.csv");

    std::vector<Row> rows;
    check_true(read_all(reader, 7, rows), "read large file in batches of 7");
    check_equal(rows.size(), static_cast<size_t>(row_count), "large-file row count");
    if (!rows.empty()) {
      check_equal(rows.front()[0], std::string("0"), "large-file first id");
      check_equal(rows.back()[0], std::string("19999"), "large-file last id");
    }

    check_equal(reader.rewind(), 0, "rewind the large file");
    std::vector<Row> first_batch;
    check_equal(reader.next_batch(3, first_batch), 0, "first batch after rewind");
    check_equal(first_batch.size(), static_cast<size_t>(3),
                "next_batch respects the requested capacity");
    if (!first_batch.empty()) {
      check_equal(first_batch[0][0], std::string("0"), "rewound first id");
    }
  }

  reader.close();
  remove_temp(path);
}

void test_large_field()
{
  GroupGuard guard("quoted field larger than the 64 KiB read buffer");

  const std::string large_value(200000, 'x');
  const std::string content = "v,w\n\"" + large_value + "\",tail\n";
  const std::string path = write_temp("large_field.csv", content);

  ObCSVReader reader;
  check_equal(reader.open(path), 0, "open large-field file");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer large-field schema");
    check_schema(reader.columns(),
                 {{"v", ObFileSqlType::VARCHAR, false},
                  {"w", ObFileSqlType::VARCHAR, false}},
                 "large_field.csv");
    std::vector<Row> rows;
    check_true(read_all(reader, 10, rows), "read large-field rows");
    check_equal(rows.size(), static_cast<size_t>(1), "large-field row count");
    if (rows.size() == 1) {
      check_equal(rows[0][0].size(), large_value.size(), "large field length");
      check_true(rows[0][0] == large_value, "large field content");
      check_equal(rows[0][1], std::string("tail"), "field after the large field");
    }
  }

  reader.close();
  remove_temp(path);
}


void test_json_basic_schema(const std::string &path)
{
  GroupGuard guard("basic.jsonl union schema and nullability");

  ObJsonReader reader;
  check_equal(reader.open(path), 0, "open basic.jsonl");
  if (!reader.is_open()) {
    return;
  }
  check_equal(reader.infer_schema(), 0, "infer_schema basic.jsonl");
  check_equal(reader.scanned_row_count(), static_cast<int64_t>(3),
              "json infer_schema scans the whole file");

  const std::vector<Column> &columns = reader.columns();
  print_schema(columns);
  check_schema(columns,
               {{"id", ObFileSqlType::BIGINT, false},
                {"name", ObFileSqlType::VARCHAR, false},
                {"score", ObFileSqlType::DOUBLE, true},
                {"active", ObFileSqlType::BOOLEAN, false},
                {"day", ObFileSqlType::DATE, true},
                {"note", ObFileSqlType::VARCHAR, true}},
               "basic.jsonl");
  reader.close();
}

void test_json_basic_rows(const std::string &path)
{
  GroupGuard guard("basic.jsonl row decoding with missing keys");

  ObJsonReader reader;
  check_equal(reader.open(path), 0, "open basic.jsonl");
  if (!reader.is_open()) {
    return;
  }
  check_equal(reader.infer_schema(), 0, "infer_schema basic.jsonl");

  std::vector<Row> rows;
  check_true(read_all(reader, 2, rows), "read basic.jsonl in batches of 2");
  check_equal(rows.size(), static_cast<size_t>(3), "basic.jsonl record count");
  if (rows.size() != 3) {
    return;
  }
  for (size_t i = 0; i < rows.size(); ++i) {
    check_equal(rows[i].size(), static_cast<size_t>(6),
                "json row " + std::to_string(i + 1) + " field count");
  }

  check_equal(rows[0][0], std::string("1"), "json row 1 id");
  check_equal(rows[0][1], std::string("Alice"), "json row 1 name");
  check_equal(rows[0][2], std::string("98.5"), "json row 1 score");
  check_equal(rows[0][3], std::string("true"), "json row 1 active");
  check_true(rows[0].is_null(4), "json row 1 day is NULL");
  check_true(rows[0].is_null(5), "json row 1 note is NULL");

  check_equal(rows[1][0], std::string("2"), "json row 2 id");
  check_equal(rows[1][1], std::string("Bob"), "json row 2 name");
  check_true(rows[1].is_null(2), "json row 2 score is NULL");
  check_equal(rows[1][3], std::string("false"), "json row 2 active");
  check_equal(rows[1][4], std::string("2026-08-02"), "json row 2 day");
  check_true(rows[1].is_null(5), "json row 2 note is NULL");

  check_equal(rows[2][0], std::string("3"), "json row 3 id");
  check_equal(rows[2][1], std::string("Chen"), "json row 3 name");
  check_equal(rows[2][2], std::string("90"), "json row 3 score");
  check_equal(rows[2][3], std::string("true"), "json row 3 active");
  check_true(rows[2].is_null(4), "json row 3 day is NULL");
  check_equal(rows[2][5], std::string("ok"), "json row 3 note");

  // EOF / rewind behaviour.
  std::vector<Row> extra;
  check_equal(reader.next_batch(1, extra), 0, "json next_batch at EOF");
  check_true(extra.empty(), "json next_batch at EOF is empty");
  check_true(reader.eof(), "json eof flag");
  check_equal(reader.rewind(), 0, "json rewind");
  check_true(!reader.eof(), "json rewind clears eof");
  check_equal(reader.next_batch(1, extra), 0, "json next_batch after rewind");
  check_equal(extra.size(), static_cast<size_t>(1), "json one row after rewind");
  if (!extra.empty()) {
    check_equal(extra[0][0], std::string("1"), "json rewind restarts at record 1");
  }
  reader.close();
}

void test_json_nested_rejected()
{
  GroupGuard guard("nested JSON object / array are rejected with a clear error");

  const std::string nested_path = write_temp(
      "nested.jsonl", "{\"a\":1,\"nested\":{\"b\":2}}\n");
  ObJsonReader nested_reader;
  check_equal(nested_reader.open(nested_path), 0, "open nested JSONL");
  if (nested_reader.is_open()) {
    check_equal(nested_reader.infer_schema(), -1, "nested JSON object is rejected");
    check_true(nested_reader.last_error() == ObJsonReadError::NESTED,
               "nested JSON object sets the NESTED error");
    check_true(nested_reader.last_error_message().find("nested JSON object") !=
                   std::string::npos,
               "nested JSON object error message mentions the object");
  }
  nested_reader.close();
  remove_temp(nested_path);

  const std::string array_path = write_temp(
      "array.jsonl", "{\"a\":[1,2,3]}\n");
  ObJsonReader array_reader;
  check_equal(array_reader.open(array_path), 0, "open array JSONL");
  if (array_reader.is_open()) {
    check_equal(array_reader.infer_schema(), -1, "JSON array value is rejected");
    check_true(array_reader.last_error() == ObJsonReadError::NESTED,
               "JSON array sets the NESTED error");
  }
  array_reader.close();
  remove_temp(array_path);
}

void test_json_widening_and_nullability()
{
  GroupGuard guard("JSON widening and missing-key nullability");

  const std::string double_path = write_temp(
      "json_double.jsonl", "{\"n\":1}\n{\"n\":2.5}\n");
  ObJsonReader double_reader;
  check_equal(double_reader.open(double_path), 0, "open JSON widening file");
  if (double_reader.is_open()) {
    check_equal(double_reader.infer_schema(), 0, "infer JSON widening schema");
    check_schema(double_reader.columns(), {{"n", ObFileSqlType::DOUBLE, false}},
                 "json widening");
  }
  double_reader.close();
  remove_temp(double_path);

  const std::string varchar_path = write_temp(
      "json_varchar.jsonl", "{\"n\":1}\n{\"n\":\"x\"}\n");
  ObJsonReader varchar_reader;
  check_equal(varchar_reader.open(varchar_path), 0, "open JSON conflict file");
  if (varchar_reader.is_open()) {
    check_equal(varchar_reader.infer_schema(), 0, "infer JSON conflict schema");
    check_schema(varchar_reader.columns(), {{"n", ObFileSqlType::VARCHAR, false}},
                 "json conflict");
  }
  varchar_reader.close();
  remove_temp(varchar_path);

  const std::string nullable_path = write_temp(
      "json_nullable.jsonl", "{\"a\":1,\"b\":2}\n{\"a\":3}\n");
  ObJsonReader nullable_reader;
  check_equal(nullable_reader.open(nullable_path), 0, "open JSON nullable file");
  if (nullable_reader.is_open()) {
    check_equal(nullable_reader.infer_schema(), 0, "infer JSON nullable schema");
    check_schema(nullable_reader.columns(),
                 {{"a", ObFileSqlType::BIGINT, false},
                  {"b", ObFileSqlType::BIGINT, true}},
                 "json nullable");
    std::vector<Row> rows;
    check_true(read_all(nullable_reader, 10, rows), "read JSON nullable rows");
    check_equal(rows.size(), static_cast<size_t>(2), "json nullable row count");
    if (rows.size() == 2) {
      check_equal(rows[0][1], std::string("2"), "json present key value");
      check_true(rows[1].is_null(1), "json missing key becomes NULL");
    }
  }
  nullable_reader.close();
  remove_temp(nullable_path);

  const std::string null_path = write_temp(
      "json_null.jsonl", "{\"a\":null,\"b\":1}\n{\"b\":2}\n");
  ObJsonReader null_reader;
  check_equal(null_reader.open(null_path), 0, "open JSON null file");
  if (null_reader.is_open()) {
    check_equal(null_reader.infer_schema(), 0, "infer JSON null schema");
    check_schema(null_reader.columns(),
                 {{"a", ObFileSqlType::VARCHAR, true},
                  {"b", ObFileSqlType::BIGINT, false}},
                 "json null");
  }
  null_reader.close();
  remove_temp(null_path);
}

void test_json_escapes_crlf_and_blank_lines()
{
  GroupGuard guard("JSON escapes, CRLF and blank lines");

  const std::string content =
      "{\"s\":\"a\\\"b\",\"n\":1}\r\n"
      "\r\n"
      "{\"s\":\"line\\nnext\",\"n\":2}\r\n";
  const std::string path = write_temp("json_escapes.jsonl", content);

  ObJsonReader reader;
  check_equal(reader.open(path), 0, "open escaped JSONL");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer escaped JSONL schema");
    check_equal(reader.scanned_row_count(), static_cast<int64_t>(2),
                "blank lines are not records");
    check_schema(reader.columns(),
                 {{"s", ObFileSqlType::VARCHAR, false},
                  {"n", ObFileSqlType::BIGINT, false}},
                 "json escapes");

    std::vector<Row> rows;
    check_true(read_all(reader, 10, rows), "read escaped JSONL");
    check_equal(rows.size(), static_cast<size_t>(2), "escaped JSONL row count");
    if (rows.size() == 2) {
      check_equal(rows[0][0], std::string("a\"b"), "escaped quote");
      check_equal(rows[1][0], std::string("line\nnext"), "escaped newline");
      check_true(rows[1][0].find('\r') == std::string::npos,
                 "CRLF does not leak into a JSON string");
    }
  }
  reader.close();
  remove_temp(path);
}


void test_parquet_basic(const std::string &path)
{
  GroupGuard guard("simple.parquet decoding (7 rows, values 0..6)");

  ObParquetReader reader;
  check_equal(reader.open(path), 0, "open simple.parquet");
  if (!reader.is_open()) {
    return;
  }
  check_equal(reader.infer_schema(), 0, "infer_schema simple.parquet");
  check_equal(reader.scanned_row_count(), static_cast<int64_t>(7),
              "parquet footer row count");
  check_schema(reader.columns(), {{"c1", ObFileSqlType::BIGINT, true}},
               "simple.parquet");

  std::vector<Row> rows;
  check_true(read_all(reader, 3, rows), "read simple.parquet in batches of 3");
  check_equal(rows.size(), static_cast<size_t>(7), "simple.parquet row count");
  int64_t sum = 0;
  for (size_t i = 0; i < rows.size(); ++i) {
    int64_t value = -1;
    check_true(rows[i].size() == 1, "parquet row has one field");
    check_true(!rows[i].is_null(0), "parquet value is not NULL");
    if (!rows[i].is_null(0) && ob_file_sql_parse_bigint(rows[i][0], value)) {
      check_equal(value, static_cast<int64_t>(i), "parquet value equals its row index");
      sum += value;
    } else {
      ++g_failures;
    }
  }
  check_equal(sum, static_cast<int64_t>(21), "parquet values sum to 21");

  // Rewind / EOF.
  std::vector<Row> extra;
  check_equal(reader.next_batch(1, extra), 0, "parquet next_batch at EOF");
  check_true(extra.empty(), "parquet next_batch at EOF is empty");
  check_true(reader.eof(), "parquet eof flag");
  check_equal(reader.rewind(), 0, "parquet rewind");
  check_true(!reader.eof(), "parquet rewind clears eof");
  check_equal(reader.next_batch(1, extra), 0, "parquet next_batch after rewind");
  check_equal(extra.size(), static_cast<size_t>(1), "parquet one row after rewind");
  if (!extra.empty()) {
    check_equal(extra[0][0], std::string("0"), "parquet rewind restarts at row 0");
  }
  reader.close();
}

void test_parquet_projection(const std::string &path)
{
  GroupGuard guard("Parquet column projection and unknown column");

  ObParquetOptions options;
  options.projected_columns.push_back("c1");
  ObParquetReader reader;
  check_equal(reader.open(path, options), 0, "open parquet with projection");
  if (reader.is_open()) {
    check_equal(reader.infer_schema(), 0, "infer parquet with projection");
    std::vector<Row> rows;
    check_true(read_all(reader, 10, rows), "read projected parquet");
    check_equal(rows.size(), static_cast<size_t>(7), "projected parquet row count");
    if (!rows.empty()) {
      check_equal(rows[0][0], std::string("0"), "projected column value");
    }
  }
  reader.close();

  ObParquetOptions bad_options;
  bad_options.projected_columns.push_back("missing_column");
  ObParquetReader bad_reader;
  check_equal(bad_reader.open(path, bad_options), 0, "open parquet with bad projection");
  if (bad_reader.is_open()) {
    check_equal(bad_reader.infer_schema(), -1, "unknown projected column is rejected");
    check_true(bad_reader.last_error() == ObParquetReadError::UNSUPPORTED,
               "unknown projected column sets UNSUPPORTED");
  }
  bad_reader.close();
}

} // namespace

int main(int argc, char **argv)
{
  const std::string default_path =
      "/home/concealher/seekdb-build/tools/deploy/mysql_test/test_suite/"
      "file_sql/data/basic.csv";
  const std::string basic_path = argc > 1 ? argv[1] : default_path;

  std::ifstream probe(basic_path.c_str(), std::ios::binary);
  if (!probe.is_open()) {
    std::cout << "FAIL: cannot open basic.csv at " << basic_path << "\n";
    return 2;
  }
  probe.close();

  const std::string json_path =
      argc > 2 ? std::string(argv[2])
               : basic_path.substr(0, basic_path.find_last_of('/') + 1) + "basic.jsonl";
  std::ifstream json_probe(json_path.c_str(), std::ios::binary);
  if (!json_probe.is_open()) {
    std::cout << "FAIL: cannot open basic.jsonl at " << json_path << "\n";
    return 2;
  }
  json_probe.close();

  std::cout << "OceanBase file_sql CSV/JSON reader self-test\n";
  std::cout << "basic.csv: " << basic_path << "\n";
  std::cout << "basic.jsonl: " << json_path << "\n";

  test_basic_schema(basic_path);
  test_basic_rows(basic_path);
  test_crlf_and_quotes();
  test_empty_column();
  test_widening();
  test_headerless();
  test_headers_options_and_bom(basic_path);
  test_nullstr_and_trim();
  test_classification_edges();
  test_streaming_large();
  test_large_field();
  test_json_basic_schema(json_path);
  test_json_basic_rows(json_path);
  test_json_nested_rejected();
  test_json_widening_and_nullability();
  test_json_escapes_crlf_and_blank_lines();

  const std::string parquet_path = parquet_fixture_path(basic_path);
  std::ifstream parquet_probe(parquet_path.c_str(), std::ios::binary);
  if (!parquet_probe.is_open()) {
    std::cout << "FAIL: cannot open simple.parquet at " << parquet_path << "\n";
    return 2;
  }
  parquet_probe.close();
  test_parquet_basic(parquet_path);
  test_parquet_projection(parquet_path);

  std::cout << "\n";
  if (g_failures == 0) {
    std::cout << "PASS: " << g_checks << " checks passed\n";
    return 0;
  }
  std::cout << "FAIL: " << g_failures << " of " << g_checks << " checks failed\n";
  return 1;
}
