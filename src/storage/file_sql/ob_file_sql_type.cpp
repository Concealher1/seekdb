/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M2: deterministic CSV value type classification and widening.
 */

#include "ob_file_sql_type.h"

#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <system_error>

namespace oceanbase
{
namespace storage
{
namespace
{

inline bool is_ascii_digit(const char c)
{
  return c >= '0' && c <= '9';
}

bool equals_ignore_case(const std::string &value, const char *lower_literal)
{
  const size_t literal_len = std::strlen(lower_literal);
  if (value.size() != literal_len) {
    return false;
  }
  for (size_t i = 0; i < literal_len; ++i) {
    char c = value[i];
    if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
    if (c != lower_literal[i]) {
      return false;
    }
  }
  return true;
}

bool is_leap_year(const int32_t year)
{
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

int32_t days_in_month(const int32_t year, const int32_t month)
{
  static const int32_t days[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month == 2 && is_leap_year(year)) {
    return 29;
  }
  return days[month - 1];
}

bool parse_two_digits(const std::string &cell, const size_t offset, int32_t &value)
{
  if (offset + 1 >= cell.size() ||
      !is_ascii_digit(cell[offset]) || !is_ascii_digit(cell[offset + 1])) {
    return false;
  }
  value = (cell[offset] - '0') * 10 + (cell[offset + 1] - '0');
  return true;
}

bool parse_four_digits(const std::string &cell, const size_t offset, int32_t &value)
{
  if (offset + 3 >= cell.size()) {
    return false;
  }
  value = 0;
  for (size_t i = 0; i < 4; ++i) {
    if (!is_ascii_digit(cell[offset + i])) {
      return false;
    }
    value = value * 10 + (cell[offset + i] - '0');
  }
  return true;
}

bool is_double_syntax(const std::string &cell)
{
  size_t pos = 0;
  if (pos < cell.size() && (cell[pos] == '+' || cell[pos] == '-')) {
    ++pos;
  }

  size_t integer_digits = 0;
  while (pos < cell.size() && is_ascii_digit(cell[pos])) {
    ++pos;
    ++integer_digits;
  }

  size_t fraction_digits = 0;
  if (pos < cell.size() && cell[pos] == '.') {
    ++pos;
    while (pos < cell.size() && is_ascii_digit(cell[pos])) {
      ++pos;
      ++fraction_digits;
    }
  }

  if (integer_digits + fraction_digits == 0) {
    return false;
  }

  if (pos < cell.size() && (cell[pos] == 'e' || cell[pos] == 'E')) {
    ++pos;
    if (pos < cell.size() && (cell[pos] == '+' || cell[pos] == '-')) {
      ++pos;
    }
    size_t exponent_digits = 0;
    while (pos < cell.size() && is_ascii_digit(cell[pos])) {
      ++pos;
      ++exponent_digits;
    }
    if (exponent_digits == 0) {
      return false;
    }
  }

  return pos == cell.size();
}

} // namespace

const char *ob_file_sql_type_name(const ObFileSqlType type)
{
  switch (type) {
    case ObFileSqlType::BOOLEAN:
      return "BOOLEAN";
    case ObFileSqlType::BIGINT:
      return "BIGINT";
    case ObFileSqlType::DOUBLE:
      return "DOUBLE";
    case ObFileSqlType::VARCHAR:
      return "VARCHAR";
    case ObFileSqlType::DATE:
      return "DATE";
    case ObFileSqlType::DATETIME:
      return "DATETIME";
    case ObFileSqlType::NULLTYPE:
      return "NULLTYPE";
    default:
      return "UNKNOWN";
  }
}

bool ob_file_sql_parse_boolean(const std::string &cell, bool &value)
{
  if (equals_ignore_case(cell, "true")) {
    value = true;
    return true;
  }
  if (equals_ignore_case(cell, "false")) {
    value = false;
    return true;
  }
  return false;
}

bool ob_file_sql_parse_bigint(const std::string &cell, int64_t &value)
{
  if (cell.empty()) {
    return false;
  }

  size_t pos = 0;
  if (cell[0] == '+' || cell[0] == '-') {
    pos = 1;
  }
  if (pos >= cell.size()) {
    return false;
  }
  for (size_t i = pos; i < cell.size(); ++i) {
    if (!is_ascii_digit(cell[i])) {
      return false;
    }
  }

  // std::from_chars does not accept a leading '+', so skip it ourselves.  It
  // does perform the full int64 range check for us.
  const char *begin = cell.c_str() + (cell[0] == '+' ? 1 : 0);
  const char *end = cell.c_str() + cell.size();
  int64_t parsed = 0;
  const std::from_chars_result result = std::from_chars(begin, end, parsed);
  if (result.ec != std::errc() || result.ptr != end) {
    return false;
  }
  value = parsed;
  return true;
}

bool ob_file_sql_parse_double(const std::string &cell, double &value)
{
  if (!is_double_syntax(cell)) {
    return false;
  }

  // The syntax has already been validated; strtod gives us the exact same
  // rounding as the C library used by the rest of the engine.
  const char *begin = cell.c_str() + (cell[0] == '+' ? 1 : 0);
  char *end = nullptr;
  errno = 0;
  const double parsed = std::strtod(begin, &end);
  if (end != cell.c_str() + cell.size()) {
    return false;
  }
  // "1e999" matches the syntax but is not a representable DOUBLE.  Treat it
  // as VARCHAR instead of silently producing +/-inf.
  if (!std::isfinite(parsed)) {
    return false;
  }
  value = parsed;
  return true;
}

bool ob_file_sql_parse_date(const std::string &cell, ObFileSqlDate &value)
{
  if (cell.size() != 10 || cell[4] != '-' || cell[7] != '-') {
    return false;
  }

  int32_t year = 0;
  int32_t month = 0;
  int32_t day = 0;
  if (!parse_four_digits(cell, 0, year) ||
      !parse_two_digits(cell, 5, month) ||
      !parse_two_digits(cell, 8, day)) {
    return false;
  }

  // Years 1..9999; year 0000 is not a valid SQL / Gregorian date.  This also
  // keeps days_in_month() in its valid range.
  if (year < 1 || year > 9999 || month < 1 || month > 12 ||
      day < 1 || day > days_in_month(year, month)) {
    return false;
  }

  value.year = year;
  value.month = month;
  value.day = day;
  return true;
}

bool ob_file_sql_parse_datetime(const std::string &cell, ObFileSqlDateTime &value)
{
  // Minimum form: YYYY-MM-DD HH:MM:SS
  if (cell.size() < 19) {
    return false;
  }
  if (cell[10] != ' ' && cell[10] != 'T') {
    return false;
  }
  if (cell[13] != ':' || cell[16] != ':') {
    return false;
  }

  ObFileSqlDate date;
  if (!ob_file_sql_parse_date(cell.substr(0, 10), date)) {
    return false;
  }

  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  if (!parse_two_digits(cell, 11, hour) ||
      !parse_two_digits(cell, 14, minute) ||
      !parse_two_digits(cell, 17, second)) {
    return false;
  }
  if (hour > 23 || minute > 59 || second > 59) {
    return false;
  }

  int32_t microsecond = 0;
  size_t pos = 19;
  if (pos < cell.size()) {
    if (cell[pos] != '.') {
      return false;
    }
    ++pos;
    if (pos >= cell.size()) {
      return false;
    }
    int32_t fraction_digits = 0;
    for (; pos < cell.size(); ++pos) {
      if (!is_ascii_digit(cell[pos])) {
        return false;
      }
      if (fraction_digits < 6) {
        microsecond = microsecond * 10 + (cell[pos] - '0');
      }
      ++fraction_digits;
    }
    for (int32_t i = fraction_digits; i < 6; ++i) {
      microsecond *= 10;
    }
  }

  value.date = date;
  value.hour = hour;
  value.minute = minute;
  value.second = second;
  value.microsecond = microsecond;
  return true;
}

ObFileSqlType ob_file_sql_classify(const std::string &cell)
{
  if (cell.empty()) {
    // An empty field is NULL; NULLTYPE means "no type observed yet".
    return ObFileSqlType::NULLTYPE;
  }

  bool boolean_value = false;
  if (ob_file_sql_parse_boolean(cell, boolean_value)) {
    return ObFileSqlType::BOOLEAN;
  }

  int64_t bigint_value = 0;
  if (ob_file_sql_parse_bigint(cell, bigint_value)) {
    return ObFileSqlType::BIGINT;
  }

  double double_value = 0;
  if (ob_file_sql_parse_double(cell, double_value)) {
    return ObFileSqlType::DOUBLE;
  }

  ObFileSqlDate date_value;
  if (ob_file_sql_parse_date(cell, date_value)) {
    return ObFileSqlType::DATE;
  }

  ObFileSqlDateTime datetime_value;
  if (ob_file_sql_parse_datetime(cell, datetime_value)) {
    return ObFileSqlType::DATETIME;
  }

  return ObFileSqlType::VARCHAR;
}

ObFileSqlType ob_file_sql_widen(const ObFileSqlType type, const ObFileSqlType other)
{
  if (type == ObFileSqlType::NULLTYPE) {
    return other;
  }
  if (other == ObFileSqlType::NULLTYPE) {
    return type;
  }
  if (type == other) {
    return type;
  }
  if ((type == ObFileSqlType::BIGINT && other == ObFileSqlType::DOUBLE) ||
      (type == ObFileSqlType::DOUBLE && other == ObFileSqlType::BIGINT)) {
    return ObFileSqlType::DOUBLE;
  }
  // Any other mixture falls back to VARCHAR.  Note that this is the literal
  // M2 rule; there is no implicit DATE -> DATETIME promotion.
  return ObFileSqlType::VARCHAR;
}

ObFileSqlType ob_file_sql_finalize(const ObFileSqlType type)
{
  return type == ObFileSqlType::NULLTYPE ? ObFileSqlType::VARCHAR : type;
}

} // namespace storage
} // namespace oceanbase
