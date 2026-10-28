/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M4: streaming newline-delimited JSON parser and schema inference.
 *
 * Each line is parsed independently, so the file is never loaded as a whole.
 * infer_schema() parses every record, keeps only the union of the keys and the
 * observed value types, then rewinds the stream; next_batch() re-parses at
 * most `capacity` lines at a time.
 */

#include "ob_json_reader.h"

#include <fstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace oceanbase
{
namespace storage
{
namespace
{

/** One decoded member of a flat JSON object. */
struct JsonField
{
  std::string name;
  std::string value;
  bool is_null = false;
};

bool json_is_ascii_digit(const char c)
{
  return c >= '0' && c <= '9';
}

bool json_is_space(const char c)
{
  return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

bool json_is_blank_line(const std::string &line)
{
  for (size_t i = 0; i < line.size(); ++i) {
    if (!json_is_space(line[i])) {
      return false;
    }
  }
  return true;
}

void json_append_utf8(const uint32_t code_point, std::string &out)
{
  if (code_point <= 0x7F) {
    out.push_back(static_cast<char>(code_point));
  } else if (code_point <= 0x7FF) {
    out.push_back(static_cast<char>(0xC0 | (code_point >> 6)));
    out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  } else if (code_point <= 0xFFFF) {
    out.push_back(static_cast<char>(0xE0 | (code_point >> 12)));
    out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  } else {
    out.push_back(static_cast<char>(0xF0 | (code_point >> 18)));
    out.push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
    out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
  }
}

/**
 * A strict parser for one line holding one flat JSON object.
 *
 * Only scalar member values are accepted.  An object or array value is
 * reported as ObJsonReadError::NESTED so that the SQL layer can raise a clear
 * "nested JSON object or array" error instead of a generic parse failure.
 */
class JsonLineParser
{
public:
  explicit JsonLineParser(const std::string &text) : text_(text), pos_(0)
  {
  }

  bool parse(std::vector<JsonField> &fields,
             ObJsonReadError &error,
             std::string &message)
  {
    fields.clear();
    error = ObJsonReadError::NONE;
    message.clear();

    skip_space();
    if (!consume('{')) {
      return fail(ObJsonReadError::PARSE, "expected a JSON object", error, message);
    }
    skip_space();
    if (consume('}')) {
      skip_space();
      if (pos_ != text_.size()) {
        return fail(ObJsonReadError::PARSE, "unexpected trailing characters", error, message);
      }
      return true;
    }

    for (;;) {
      skip_space();
      std::string name;
      if (!parse_string(name)) {
        return fail(ObJsonReadError::PARSE, "expected a double-quoted member name", error, message);
      }
      skip_space();
      if (!consume(':')) {
        return fail(ObJsonReadError::PARSE, "expected ':' after a member name", error, message);
      }
      skip_space();

      JsonField field;
      field.name = name;
      if (peek() == '{') {
        return fail(ObJsonReadError::NESTED, "nested JSON object", error, message);
      }
      if (peek() == '[') {
        return fail(ObJsonReadError::NESTED, "JSON array", error, message);
      }
      if (!parse_scalar(field)) {
        return fail(ObJsonReadError::PARSE, "invalid JSON scalar value", error, message);
      }

      // A duplicate member inside one object keeps the first position but the
      // last value, which matches "last key wins" semantics.
      bool replaced = false;
      for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i].name == field.name) {
          fields[i] = field;
          replaced = true;
          break;
        }
      }
      if (!replaced) {
        fields.push_back(std::move(field));
      }

      skip_space();
      if (consume(',')) {
        continue;
      }
      if (consume('}')) {
        break;
      }
      return fail(ObJsonReadError::PARSE, "expected ',' or '}'", error, message);
    }

    skip_space();
    if (pos_ != text_.size()) {
      return fail(ObJsonReadError::PARSE, "unexpected trailing characters", error, message);
    }
    return true;
  }

private:
  bool fail(const ObJsonReadError code,
            const char *what,
            ObJsonReadError &error,
            std::string &message) const
  {
    error = code;
    message = what;
    return false;
  }

  int peek() const
  {
    return pos_ < text_.size() ? static_cast<unsigned char>(text_[pos_]) : -1;
  }

  void skip_space()
  {
    while (pos_ < text_.size() && json_is_space(text_[pos_])) {
      ++pos_;
    }
  }

  bool consume(const char expected)
  {
    if (pos_ < text_.size() && text_[pos_] == expected) {
      ++pos_;
      return true;
    }
    return false;
  }

  bool parse_scalar(JsonField &field)
  {
    const int c = peek();
    if (c < 0) {
      return false;
    }
    if (c == '"') {
      return parse_string(field.value);
    }
    if (c == 't') {
      if (!consume_literal("true")) {
        return false;
      }
      field.value = "true";
      return true;
    }
    if (c == 'f') {
      if (!consume_literal("false")) {
        return false;
      }
      field.value = "false";
      return true;
    }
    if (c == 'n') {
      if (!consume_literal("null")) {
        return false;
      }
      field.is_null = true;
      field.value.clear();
      return true;
    }
    if (c == '-' || json_is_ascii_digit(static_cast<char>(c))) {
      return parse_number(field.value);
    }
    return false;
  }

  bool consume_literal(const char *literal)
  {
    const size_t length = std::char_traits<char>::length(literal);
    if (text_.compare(pos_, length, literal) != 0) {
      return false;
    }
    pos_ += length;
    return true;
  }

  bool parse_number(std::string &out)
  {
    const size_t start = pos_;
    if (peek() == '-') {
      ++pos_;
    }
    if (pos_ >= text_.size()) {
      return false;
    }
    if (text_[pos_] == '0') {
      ++pos_;
      if (pos_ < text_.size() && json_is_ascii_digit(text_[pos_])) {
        return false;
      }
    } else if (text_[pos_] >= '1' && text_[pos_] <= '9') {
      while (pos_ < text_.size() && json_is_ascii_digit(text_[pos_])) {
        ++pos_;
      }
    } else {
      return false;
    }

    if (pos_ < text_.size() && text_[pos_] == '.') {
      ++pos_;
      if (pos_ >= text_.size() || !json_is_ascii_digit(text_[pos_])) {
        return false;
      }
      while (pos_ < text_.size() && json_is_ascii_digit(text_[pos_])) {
        ++pos_;
      }
    }

    if (pos_ < text_.size() && (text_[pos_] == 'e' || text_[pos_] == 'E')) {
      ++pos_;
      if (pos_ < text_.size() && (text_[pos_] == '+' || text_[pos_] == '-')) {
        ++pos_;
      }
      if (pos_ >= text_.size() || !json_is_ascii_digit(text_[pos_])) {
        return false;
      }
      while (pos_ < text_.size() && json_is_ascii_digit(text_[pos_])) {
        ++pos_;
      }
    }

    out.assign(text_, start, pos_ - start);
    return true;
  }

  bool parse_string(std::string &out)
  {
    out.clear();
    if (!consume('"')) {
      return false;
    }
    while (pos_ < text_.size()) {
      const unsigned char c = static_cast<unsigned char>(text_[pos_++]);
      if (c == '"') {
        return true;
      }
      if (c == '\\') {
        if (!parse_escape(out)) {
          return false;
        }
      } else if (c < 0x20) {
        // A raw control character is not valid inside a JSON string.
        return false;
      } else {
        out.push_back(static_cast<char>(c));
      }
    }
    return false;
  }

  bool parse_escape(std::string &out)
  {
    if (pos_ >= text_.size()) {
      return false;
    }
    const char escape = text_[pos_++];
    switch (escape) {
      case '"':
        out.push_back('"');
        return true;
      case '\\':
        out.push_back('\\');
        return true;
      case '/':
        out.push_back('/');
        return true;
      case 'b':
        out.push_back('\b');
        return true;
      case 'f':
        out.push_back('\f');
        return true;
      case 'n':
        out.push_back('\n');
        return true;
      case 'r':
        out.push_back('\r');
        return true;
      case 't':
        out.push_back('\t');
        return true;
      case 'u': {
        uint32_t code_point = 0;
        if (!parse_hex4(code_point)) {
          return false;
        }
        if (code_point >= 0xD800 && code_point <= 0xDBFF) {
          // High surrogate: a low surrogate escape must follow.
          if (pos_ + 1 >= text_.size() || text_[pos_] != '\\' || text_[pos_ + 1] != 'u') {
            return false;
          }
          pos_ += 2;
          uint32_t low = 0;
          if (!parse_hex4(low) || low < 0xDC00 || low > 0xDFFF) {
            return false;
          }
          code_point = 0x10000 + ((code_point - 0xD800) << 10) + (low - 0xDC00);
        } else if (code_point >= 0xDC00 && code_point <= 0xDFFF) {
          // A lone low surrogate is not a valid code point.
          return false;
        }
        json_append_utf8(code_point, out);
        return true;
      }
      default:
        return false;
    }
  }

  bool parse_hex4(uint32_t &value)
  {
    if (pos_ + 4 > text_.size()) {
      return false;
    }
    value = 0;
    for (size_t i = 0; i < 4; ++i) {
      const char c = text_[pos_ + i];
      uint32_t digit = 0;
      if (c >= '0' && c <= '9') {
        digit = static_cast<uint32_t>(c - '0');
      } else if (c >= 'a' && c <= 'f') {
        digit = static_cast<uint32_t>(c - 'a' + 10);
      } else if (c >= 'A' && c <= 'F') {
        digit = static_cast<uint32_t>(c - 'A' + 10);
      } else {
        return false;
      }
      value = (value << 4) | digit;
    }
    pos_ += 4;
    return true;
  }

  const std::string &text_;
  size_t pos_;
};

} // namespace

struct ObJsonReader::Impl
{
  ~Impl()
  {
    close();
  }

  void close()
  {
    stream.clear();
    if (stream.is_open()) {
      stream.close();
    }
    columns.clear();
    column_index.clear();
    data_offset = 0;
    line_number = 0;
    opened = false;
    eof = false;
    schema_inferred = false;
    scanned_rows = 0;
    error = ObJsonReadError::NONE;
    error_message.clear();
  }

  void set_error(const ObJsonReadError code, const std::string &message)
  {
    error = code;
    error_message = message;
  }

  int reset_to_start()
  {
    stream.clear();
    stream.seekg(static_cast<std::streamoff>(data_offset), std::ios::beg);
    if (!stream.good()) {
      set_error(ObJsonReadError::IO, "failed to seek the JSON file");
      return -1;
    }
    line_number = 0;
    eof = false;
    return 0;
  }

  /**
   * Read one physical line.  Sets has_line=false at clean EOF.  Returns false
   * only on a real I/O error.
   */
  bool read_line(std::string &line, bool &has_line)
  {
    line.clear();
    has_line = false;
    if (!std::getline(stream, line)) {
      if (stream.eof()) {
        return true;
      }
      set_error(ObJsonReadError::IO, "failed to read the JSON file");
      return false;
    }
    ++line_number;
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    has_line = true;
    return true;
  }

  bool parse_record(const std::string &line, std::vector<JsonField> &fields)
  {
    JsonLineParser parser(line);
    ObJsonReadError parse_error = ObJsonReadError::NONE;
    std::string parse_message;
    if (!parser.parse(fields, parse_error, parse_message)) {
      set_error(parse_error,
                "line " + std::to_string(line_number) + ": " + parse_message);
      return false;
    }
    return true;
  }

  std::ifstream stream;
  ObJsonOptions options;
  std::vector<Column> columns;
  std::unordered_map<std::string, size_t> column_index;
  int64_t data_offset = 0;
  int64_t line_number = 0;
  bool opened = false;
  bool eof = false;
  bool schema_inferred = false;
  int64_t scanned_rows = 0;
  ObJsonReadError error = ObJsonReadError::NONE;
  std::string error_message;
};

ObJsonReader::ObJsonReader() : impl_(new Impl())
{
}

ObJsonReader::~ObJsonReader() = default;

int ObJsonReader::open(const std::string &path, const ObJsonOptions &options)
{
  if (!impl_) {
    impl_.reset(new Impl());
  }
  impl_->close();

  impl_->stream.open(path.c_str(), std::ios::in | std::ios::binary);
  if (!impl_->stream.is_open()) {
    impl_->set_error(ObJsonReadError::OPEN, "failed to open the JSON file");
    return -1;
  }
  impl_->options = options;

  // Skip a UTF-8 BOM: it belongs to the encoding, not to the first record.
  unsigned char bom[3] = {0, 0, 0};
  impl_->stream.read(reinterpret_cast<char *>(bom), sizeof(bom));
  if (impl_->stream.gcount() == static_cast<std::streamsize>(sizeof(bom)) &&
      bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF) {
    impl_->data_offset = 3;
  }
  impl_->stream.clear();
  impl_->opened = true;
  if (impl_->reset_to_start() != 0) {
    impl_->close();
    return -1;
  }
  return 0;
}

void ObJsonReader::close()
{
  if (impl_) {
    impl_->close();
  }
}

bool ObJsonReader::is_open() const
{
  return impl_ && impl_->opened;
}

int ObJsonReader::infer_schema()
{
  if (!impl_ || !impl_->opened) {
    return -1;
  }
  Impl &state = *impl_;
  if (!state.options.newline_delimited) {
    state.set_error(ObJsonReadError::PARSE, "only newline-delimited JSON is supported");
    return -1;
  }
  if (state.reset_to_start() != 0) {
    return -1;
  }

  state.columns.clear();
  state.column_index.clear();
  state.scanned_rows = 0;
  state.schema_inferred = false;
  state.error = ObJsonReadError::NONE;
  state.error_message.clear();

  std::vector<std::string> names;
  std::vector<ObFileSqlType> types;
  std::vector<bool> nullable;
  std::unordered_map<std::string, size_t> positions;

  std::string line;
  bool has_line = false;
  while (state.read_line(line, has_line)) {
    if (!has_line) {
      break;
    }
    if (json_is_blank_line(line)) {
      continue;
    }
    std::vector<JsonField> fields;
    if (!state.parse_record(line, fields)) {
      return -1;
    }
    const bool had_previous_records = state.scanned_rows > 0;
    ++state.scanned_rows;

    // Track which of the already-known columns this record actually carries.
    // A key that is absent from a record is NULL for that row, so the column
    // must be nullable.
    std::vector<bool> present(names.size(), false);
    for (size_t i = 0; i < fields.size(); ++i) {
      const JsonField &field = fields[i];
      size_t index = 0;
      const auto position_it = positions.find(field.name);
      if (position_it == positions.end()) {
        index = names.size();
        positions.emplace(field.name, index);
        names.push_back(field.name);
        types.push_back(ObFileSqlType::NULLTYPE);
        // The key did not appear in any earlier record.
        nullable.push_back(had_previous_records);
        present.push_back(false);
      } else {
        index = position_it->second;
      }
      present[index] = true;

      if (field.is_null || field.value.empty()) {
        nullable[index] = true;
        continue;
      }
      const ObFileSqlType value_type = ob_file_sql_classify(field.value);
      types[index] = ob_file_sql_widen(types[index], value_type);
    }

    for (size_t i = 0; i < present.size(); ++i) {
      if (!present[i]) {
        nullable[i] = true;
      }
    }
  }

  for (size_t i = 0; i < names.size(); ++i) {
    Column column;
    column.name = names[i];
    column.type = ob_file_sql_finalize(types[i]);
    // With no records every column is (vacuously) nullable.
    column.nullable = nullable[i] || state.scanned_rows == 0;
    state.column_index.emplace(column.name, state.columns.size());
    state.columns.push_back(std::move(column));
  }

  state.schema_inferred = true;
  if (state.reset_to_start() != 0) {
    return -1;
  }
  return 0;
}

int ObJsonReader::next_batch(const size_t capacity, std::vector<Row> &rows)
{
  rows.clear();
  if (!impl_ || !impl_->opened) {
    return -1;
  }
  if (capacity == 0 || impl_->eof) {
    return 0;
  }
  if (!impl_->schema_inferred) {
    impl_->set_error(ObJsonReadError::PARSE, "infer_schema() must be called first");
    return -1;
  }

  rows.reserve(capacity);
  std::string line;
  bool has_line = false;
  while (rows.size() < capacity) {
    if (!impl_->read_line(line, has_line)) {
      return -1;
    }
    if (!has_line) {
      impl_->eof = true;
      break;
    }
    if (json_is_blank_line(line)) {
      continue;
    }

    std::vector<JsonField> fields;
    if (!impl_->parse_record(line, fields)) {
      return -1;
    }

    Row row;
    row.fields.resize(impl_->columns.size());
    for (size_t i = 0; i < fields.size(); ++i) {
      const auto it = impl_->column_index.find(fields[i].name);
      if (it != impl_->column_index.end() && !fields[i].is_null) {
        row.fields[it->second] = fields[i].value;
      }
    }
    rows.push_back(std::move(row));
  }

  return 0;
}

int ObJsonReader::rewind()
{
  if (!impl_ || !impl_->opened) {
    return -1;
  }
  return impl_->reset_to_start();
}

const std::vector<Column> &ObJsonReader::columns() const
{
  static const std::vector<Column> empty_columns;
  return impl_ ? impl_->columns : empty_columns;
}

const ObJsonOptions &ObJsonReader::options() const
{
  static const ObJsonOptions default_options;
  return impl_ ? impl_->options : default_options;
}

bool ObJsonReader::eof() const
{
  return impl_ && impl_->eof;
}

int64_t ObJsonReader::scanned_row_count() const
{
  return impl_ ? impl_->scanned_rows : 0;
}

ObJsonReadError ObJsonReader::last_error() const
{
  return impl_ ? impl_->error : ObJsonReadError::NONE;
}

const std::string &ObJsonReader::last_error_message() const
{
  static const std::string empty_message;
  return impl_ ? impl_->error_message : empty_message;
}

} // namespace storage
} // namespace oceanbase
