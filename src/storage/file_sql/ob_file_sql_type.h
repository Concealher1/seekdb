/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M2: standalone "file as database" value types and deterministic type
 * inference.  This module is deliberately free of any OceanBase dependency so
 * that it can be compiled and tested with a plain C++17 compiler.
 */

#ifndef OCEANBASE_STORAGE_FILE_SQL_OB_FILE_SQL_TYPE_H_
#define OCEANBASE_STORAGE_FILE_SQL_OB_FILE_SQL_TYPE_H_

#include <cstdint>
#include <string>

namespace oceanbase
{
namespace storage
{

/**
 * Value types supported by the file SQL CSV reader.
 *
 * The enum order is the "narrow to wide" order used by the specification:
 * BOOLEAN, BIGINT, DOUBLE, VARCHAR, DATE, DATETIME, NULLTYPE.  NULLTYPE is
 * only used while scanning as the "no type observed yet" marker; it is never
 * the type of a value that was actually present in the file.
 */
enum class ObFileSqlType
{
  BOOLEAN = 0,
  BIGINT,
  DOUBLE,
  VARCHAR,
  DATE,
  DATETIME,
  NULLTYPE
};

/** A column produced by CSV schema inference. */
struct Column
{
  std::string name;
  ObFileSqlType type = ObFileSqlType::NULLTYPE;
  bool nullable = false;
};

/** Alias matching the naming used in the M2 design sketch. */
using ObFileSQLColumn = Column;

/** A parsed calendar date (proleptic Gregorian calendar, years 1..9999). */
struct ObFileSqlDate
{
  int32_t year = 0;
  int32_t month = 0;
  int32_t day = 0;
};

/** A parsed date-time value. */
struct ObFileSqlDateTime
{
  ObFileSqlDate date;
  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t microsecond = 0;
};

/** Human readable type name (BOOLEAN, BIGINT, ..., NULLTYPE). */
const char *ob_file_sql_type_name(ObFileSqlType type);

/**
 * Classify one CSV cell.
 *
 * The cell is expected to be non-NULL, i.e. the caller has already handled an
 * empty field / the configured null string.  An empty string is nevertheless
 * classified as NULLTYPE for convenience.  The function never returns
 * VARCHAR for a value that matches a narrower type.
 */
ObFileSqlType ob_file_sql_classify(const std::string &cell);

/**
 * Widen two observed types.
 *
 *   NULLTYPE + T      -> T          (no type observed yet)
 *   T + T             -> T
 *   BIGINT + DOUBLE   -> DOUBLE
 *   anything else     -> VARCHAR
 */
ObFileSqlType ob_file_sql_widen(ObFileSqlType type, ObFileSqlType other);

/**
 * Final type of a column after a full scan: a column for which no value was
 * observed (all fields empty / missing) becomes VARCHAR and, at the CSV
 * reader level, nullable.
 */
ObFileSqlType ob_file_sql_finalize(ObFileSqlType type);

/**
 * Strict scalar parsers.  They accept exactly the syntax described by the M2
 * specification and additionally validate ranges / calendar dates.
 */
bool ob_file_sql_parse_boolean(const std::string &cell, bool &value);
bool ob_file_sql_parse_bigint(const std::string &cell, int64_t &value);
bool ob_file_sql_parse_double(const std::string &cell, double &value);
bool ob_file_sql_parse_date(const std::string &cell, ObFileSqlDate &value);
bool ob_file_sql_parse_datetime(const std::string &cell, ObFileSqlDateTime &value);

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_FILE_SQL_OB_FILE_SQL_TYPE_H_
