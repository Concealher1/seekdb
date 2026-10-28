/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * M4: standalone Parquet reader (flat schemas).
 *
 * Layout of this file:
 *   1. minimal Thrift compact-protocol reader
 *   2. Parquet metadata structures + parsers
 *   3. Snappy / RLE-hybrid / PLAIN decoders
 *   4. ObParquetReader::Impl (footer, schema, column chunks, rows)
 */

#include "ob_parquet_reader.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <set>
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

// ---------------------------------------------------------------------------
// 1. Thrift compact protocol
// ---------------------------------------------------------------------------

enum ParquetThriftType
{
  PARQUET_T_STOP = 0,
  PARQUET_T_TRUE = 1,
  PARQUET_T_FALSE = 2,
  PARQUET_T_BYTE = 3,
  PARQUET_T_I16 = 4,
  PARQUET_T_I32 = 5,
  PARQUET_T_I64 = 6,
  PARQUET_T_DOUBLE = 7,
  PARQUET_T_BINARY = 8,
  PARQUET_T_LIST = 9,
  PARQUET_T_SET = 10,
  PARQUET_T_MAP = 11,
  PARQUET_T_STRUCT = 12
};

class ParquetCompactReader
{
public:
  ParquetCompactReader(const uint8_t *data, const size_t size, const size_t pos = 0)
      : data_(data), size_(size), pos_(pos), ok_(true)
  {
  }

  bool ok() const
  {
    return ok_;
  }

  size_t pos() const
  {
    return pos_;
  }

  bool read_byte(uint8_t &value)
  {
    if (!ok_ || pos_ >= size_) {
      ok_ = false;
      return false;
    }
    value = data_[pos_++];
    return true;
  }

  bool read_varint(uint64_t &value)
  {
    value = 0;
    int shift = 0;
    for (;;) {
      uint8_t byte = 0;
      if (!read_byte(byte)) {
        return false;
      }
      if (shift < 64) {
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
      }
      if (0 == (byte & 0x80)) {
        return true;
      }
      shift += 7;
      if (shift > 63) {
        ok_ = false;
        return false;
      }
    }
  }

  bool read_zigzag(int64_t &value)
  {
    uint64_t raw = 0;
    if (!read_varint(raw)) {
      return false;
    }
    value = static_cast<int64_t>(raw >> 1);
    if (0 != (raw & 1)) {
      value = ~value;
    }
    return true;
  }

  bool read_i32(int32_t &value)
  {
    int64_t raw = 0;
    if (!read_zigzag(raw)) {
      return false;
    }
    value = static_cast<int32_t>(raw);
    return true;
  }

  bool read_i64(int64_t &value)
  {
    return read_zigzag(value);
  }

  bool read_binary(std::string &value)
  {
    uint64_t length = 0;
    if (!read_varint(length) || length > size_ - pos_) {
      ok_ = false;
      return false;
    }
    value.assign(reinterpret_cast<const char *>(data_ + pos_), static_cast<size_t>(length));
    pos_ += static_cast<size_t>(length);
    return true;
  }

  bool read_list(uint8_t &element_type, uint32_t &count)
  {
    uint8_t header = 0;
    if (!read_byte(header)) {
      return false;
    }
    element_type = static_cast<uint8_t>(header & 0x0F);
    uint64_t size = static_cast<uint64_t>(header >> 4);
    if (15 == size) {
      if (!read_varint(size)) {
        return false;
      }
    }
    if (size > std::numeric_limits<uint32_t>::max()) {
      ok_ = false;
      return false;
    }
    count = static_cast<uint32_t>(size);
    return true;
  }

  /**
   * Read one field header.  `stop` is set on the STOP byte.  `last_id` is the
   * previous field id inside the current struct.
   */
  bool read_field(uint8_t &type, int32_t &id, int32_t &last_id, bool &stop)
  {
    uint8_t header = 0;
    if (!read_byte(header)) {
      return false;
    }
    stop = false;
    if (0 == header) {
      stop = true;
      type = PARQUET_T_STOP;
      id = 0;
      return true;
    }
    type = static_cast<uint8_t>(header & 0x0F);
    const int32_t delta = static_cast<int32_t>(header >> 4);
    if (0 == delta) {
      int64_t explicit_id = 0;
      if (!read_zigzag(explicit_id)) {
        return false;
      }
      id = static_cast<int32_t>(explicit_id);
    } else {
      id = last_id + delta;
    }
    last_id = id;
    return true;
  }

  bool skip(const uint8_t type)
  {
    bool ret = true;
    switch (type) {
      case PARQUET_T_TRUE:
      case PARQUET_T_FALSE:
        break;
      case PARQUET_T_BYTE: {
        if (pos_ >= size_) {
          ok_ = false;
          ret = false;
        } else {
          ++pos_;
        }
        break;
      }
      case PARQUET_T_I16:
      case PARQUET_T_I32:
      case PARQUET_T_I64: {
        uint64_t ignored = 0;
        ret = read_varint(ignored);
        break;
      }
      case PARQUET_T_DOUBLE: {
        if (size_ - pos_ < 8) {
          ok_ = false;
          ret = false;
        } else {
          pos_ += 8;
        }
        break;
      }
      case PARQUET_T_BINARY: {
        std::string ignored;
        ret = read_binary(ignored);
        break;
      }
      case PARQUET_T_LIST:
      case PARQUET_T_SET: {
        uint8_t element_type = 0;
        uint32_t count = 0;
        ret = read_list(element_type, count) && skip_many(element_type, count);
        break;
      }
      case PARQUET_T_MAP: {
        uint64_t count = 0;
        ret = read_varint(count);
        if (ret && count > 0) {
          uint8_t types = 0;
          ret = read_byte(types);
          if (ret) {
            const uint8_t key_type = static_cast<uint8_t>(types >> 4);
            const uint8_t value_type = static_cast<uint8_t>(types & 0x0F);
            for (uint64_t i = 0; ret && i < count; ++i) {
              ret = skip(key_type) && skip(value_type);
            }
          }
        }
        break;
      }
      case PARQUET_T_STRUCT: {
        int32_t last_id = 0;
        bool stop = false;
        while (ret) {
          uint8_t field_type = 0;
          int32_t field_id = 0;
          ret = read_field(field_type, field_id, last_id, stop);
          if (!ret || stop) {
            break;
          }
          ret = skip(field_type);
        }
        break;
      }
      default:
        ok_ = false;
        ret = false;
        break;
    }
    return ret;
  }

private:
  bool skip_many(const uint8_t type, const uint32_t count)
  {
    bool ret = true;
    for (uint32_t i = 0; ret && i < count; ++i) {
      ret = skip(type);
    }
    return ret;
  }

  const uint8_t *data_;
  size_t size_;
  size_t pos_;
  bool ok_;
};

// ---------------------------------------------------------------------------
// 2. Parquet metadata
// ---------------------------------------------------------------------------

constexpr int32_t PARQUET_TYPE_BOOLEAN = 0;
constexpr int32_t PARQUET_TYPE_INT32 = 1;
constexpr int32_t PARQUET_TYPE_INT64 = 2;
constexpr int32_t PARQUET_TYPE_INT96 = 3;
constexpr int32_t PARQUET_TYPE_FLOAT = 4;
constexpr int32_t PARQUET_TYPE_DOUBLE = 5;
constexpr int32_t PARQUET_TYPE_BYTE_ARRAY = 6;
constexpr int32_t PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY = 7;

constexpr int32_t PARQUET_REPETITION_REQUIRED = 0;
constexpr int32_t PARQUET_REPETITION_OPTIONAL = 1;
constexpr int32_t PARQUET_REPETITION_REPEATED = 2;

constexpr int32_t PARQUET_ENCODING_PLAIN = 0;
constexpr int32_t PARQUET_ENCODING_PLAIN_DICTIONARY = 2;
constexpr int32_t PARQUET_ENCODING_RLE = 3;
constexpr int32_t PARQUET_ENCODING_BIT_PACKED = 4;
constexpr int32_t PARQUET_ENCODING_RLE_DICTIONARY = 8;

constexpr int32_t PARQUET_CODEC_UNCOMPRESSED = 0;
constexpr int32_t PARQUET_CODEC_SNAPPY = 1;

constexpr int32_t PARQUET_PAGE_DATA = 0;
constexpr int32_t PARQUET_PAGE_INDEX = 1;
constexpr int32_t PARQUET_PAGE_DICTIONARY = 2;
constexpr int32_t PARQUET_PAGE_DATA_V2 = 3;

// ConvertedType values used by the type mapping.
constexpr int32_t PARQUET_CONVERTED_UTF8 = 0;
constexpr int32_t PARQUET_CONVERTED_DATE = 6;
constexpr int32_t PARQUET_CONVERTED_TIMESTAMP_MILLIS = 9;
constexpr int32_t PARQUET_CONVERTED_TIMESTAMP_MICROS = 10;

struct ParquetSchemaElement
{
  int32_t type = -1;
  int32_t type_length = 0;
  int32_t repetition = -1;
  int32_t num_children = 0;
  int32_t converted = -1;
  std::string name;
};

struct ParquetColumnMeta
{
  int32_t type = -1;
  int32_t codec = PARQUET_CODEC_UNCOMPRESSED;
  int64_t num_values = 0;
  int64_t total_uncompressed_size = 0;
  int64_t total_compressed_size = 0;
  int64_t data_page_offset = 0;
  int64_t index_page_offset = -1;
  int64_t dictionary_page_offset = -1;
  std::vector<int32_t> encodings;
  std::vector<std::string> path;
};

struct ParquetColumnChunk
{
  std::string file_path;
  int64_t file_offset = 0;
  ParquetColumnMeta meta;
  bool has_meta = false;
};

struct ParquetRowGroup
{
  int64_t num_rows = 0;
  std::vector<ParquetColumnChunk> columns;
};

struct ParquetFileMeta
{
  int32_t version = 0;
  int64_t num_rows = 0;
  std::vector<ParquetSchemaElement> schema;
  std::vector<ParquetRowGroup> row_groups;
};

struct ParquetDataPageHeader
{
  int32_t num_values = 0;
  int32_t encoding = PARQUET_ENCODING_PLAIN;
  int32_t definition_level_encoding = PARQUET_ENCODING_RLE;
  int32_t repetition_level_encoding = PARQUET_ENCODING_RLE;
};

struct ParquetDataPageHeaderV2
{
  int32_t num_values = 0;
  int32_t num_nulls = 0;
  int32_t num_rows = 0;
  int32_t encoding = PARQUET_ENCODING_PLAIN;
  int32_t definition_levels_byte_length = 0;
  int32_t repetition_levels_byte_length = 0;
  bool is_compressed = true;
};

struct ParquetDictionaryPageHeader
{
  int32_t num_values = 0;
  int32_t encoding = PARQUET_ENCODING_PLAIN;
};

struct ParquetPageHeader
{
  int32_t type = -1;
  int32_t uncompressed_page_size = 0;
  int32_t compressed_page_size = 0;
  bool has_data_page = false;
  bool has_dictionary_page = false;
  bool has_data_page_v2 = false;
  ParquetDataPageHeader data_page;
  ParquetDataPageHeaderV2 data_page_v2;
  ParquetDictionaryPageHeader dictionary_page;
};

bool parquet_parse_schema_element(ParquetCompactReader &reader, ParquetSchemaElement &element)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1:
        ret = reader.read_i32(element.type);
        break;
      case 2:
        ret = reader.read_i32(element.type_length);
        break;
      case 3:
        ret = reader.read_i32(element.repetition);
        break;
      case 4:
        ret = reader.read_binary(element.name);
        break;
      case 5:
        ret = reader.read_i32(element.num_children);
        break;
      case 6:
        ret = reader.read_i32(element.converted);
        break;
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

bool parquet_parse_column_meta(ParquetCompactReader &reader, ParquetColumnMeta &meta)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1:
        ret = reader.read_i32(meta.type);
        break;
      case 2: {
        uint8_t element_type = 0;
        uint32_t count = 0;
        ret = reader.read_list(element_type, count);
        for (uint32_t i = 0; ret && i < count; ++i) {
          int32_t encoding = 0;
          ret = reader.read_i32(encoding);
          if (ret) {
            meta.encodings.push_back(encoding);
          }
        }
        break;
      }
      case 3: {
        uint8_t element_type = 0;
        uint32_t count = 0;
        ret = reader.read_list(element_type, count);
        for (uint32_t i = 0; ret && i < count; ++i) {
          std::string path;
          ret = reader.read_binary(path);
          if (ret) {
            meta.path.push_back(std::move(path));
          }
        }
        break;
      }
      case 4:
        ret = reader.read_i32(meta.codec);
        break;
      case 5:
        ret = reader.read_i64(meta.num_values);
        break;
      case 6:
        ret = reader.read_i64(meta.total_uncompressed_size);
        break;
      case 7:
        ret = reader.read_i64(meta.total_compressed_size);
        break;
      case 9:
        ret = reader.read_i64(meta.data_page_offset);
        break;
      case 10:
        ret = reader.read_i64(meta.index_page_offset);
        break;
      case 11:
        ret = reader.read_i64(meta.dictionary_page_offset);
        break;
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

bool parquet_parse_column_chunk(ParquetCompactReader &reader, ParquetColumnChunk &chunk)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1:
        ret = reader.read_binary(chunk.file_path);
        break;
      case 2:
        ret = reader.read_i64(chunk.file_offset);
        break;
      case 3:
        ret = parquet_parse_column_meta(reader, chunk.meta);
        chunk.has_meta = ret;
        break;
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

bool parquet_parse_row_group(ParquetCompactReader &reader, ParquetRowGroup &group)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1: {
        uint8_t element_type = 0;
        uint32_t count = 0;
        ret = reader.read_list(element_type, count);
        for (uint32_t i = 0; ret && i < count; ++i) {
          ParquetColumnChunk chunk;
          ret = parquet_parse_column_chunk(reader, chunk);
          if (ret) {
            group.columns.push_back(std::move(chunk));
          }
        }
        break;
      }
      case 3:
        ret = reader.read_i64(group.num_rows);
        break;
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

bool parquet_parse_file_meta(ParquetCompactReader &reader, ParquetFileMeta &meta)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1:
        ret = reader.read_i32(meta.version);
        break;
      case 2: {
        uint8_t element_type = 0;
        uint32_t count = 0;
        ret = reader.read_list(element_type, count);
        for (uint32_t i = 0; ret && i < count; ++i) {
          ParquetSchemaElement element;
          ret = parquet_parse_schema_element(reader, element);
          if (ret) {
            meta.schema.push_back(std::move(element));
          }
        }
        break;
      }
      case 3:
        ret = reader.read_i64(meta.num_rows);
        break;
      case 4: {
        uint8_t element_type = 0;
        uint32_t count = 0;
        ret = reader.read_list(element_type, count);
        for (uint32_t i = 0; ret && i < count; ++i) {
          ParquetRowGroup group;
          ret = parquet_parse_row_group(reader, group);
          if (ret) {
            meta.row_groups.push_back(std::move(group));
          }
        }
        break;
      }
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

bool parquet_parse_data_page_header(ParquetCompactReader &reader, ParquetDataPageHeader &header)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1:
        ret = reader.read_i32(header.num_values);
        break;
      case 2:
        ret = reader.read_i32(header.encoding);
        break;
      case 3:
        ret = reader.read_i32(header.definition_level_encoding);
        break;
      case 4:
        ret = reader.read_i32(header.repetition_level_encoding);
        break;
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

bool parquet_parse_data_page_header_v2(ParquetCompactReader &reader,
                                       ParquetDataPageHeaderV2 &header)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1:
        ret = reader.read_i32(header.num_values);
        break;
      case 2:
        ret = reader.read_i32(header.num_nulls);
        break;
      case 3:
        ret = reader.read_i32(header.num_rows);
        break;
      case 4:
        ret = reader.read_i32(header.encoding);
        break;
      case 5:
        ret = reader.read_i32(header.definition_levels_byte_length);
        break;
      case 6:
        ret = reader.read_i32(header.repetition_levels_byte_length);
        break;
      case 7:
        // Thrift compact encodes a boolean field in the field type itself.
        header.is_compressed = (PARQUET_T_TRUE == type);
        break;
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

bool parquet_parse_dictionary_page_header(ParquetCompactReader &reader,
                                          ParquetDictionaryPageHeader &header)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1:
        ret = reader.read_i32(header.num_values);
        break;
      case 2:
        ret = reader.read_i32(header.encoding);
        break;
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

bool parquet_parse_page_header(ParquetCompactReader &reader, ParquetPageHeader &header)
{
  int32_t last_id = 0;
  uint8_t type = 0;
  int32_t id = 0;
  bool stop = false;
  bool ret = true;
  while (ret) {
    ret = reader.read_field(type, id, last_id, stop);
    if (!ret || stop) {
      break;
    }
    switch (id) {
      case 1:
        ret = reader.read_i32(header.type);
        break;
      case 2:
        ret = reader.read_i32(header.uncompressed_page_size);
        break;
      case 3:
        ret = reader.read_i32(header.compressed_page_size);
        break;
      case 5:
        ret = parquet_parse_data_page_header(reader, header.data_page);
        header.has_data_page = ret;
        break;
      case 7:
        ret = parquet_parse_dictionary_page_header(reader, header.dictionary_page);
        header.has_dictionary_page = ret;
        break;
      case 8:
        ret = parquet_parse_data_page_header_v2(reader, header.data_page_v2);
        header.has_data_page_v2 = ret;
        break;
      default:
        ret = reader.skip(type);
        break;
    }
  }
  return ret && reader.ok();
}

// ---------------------------------------------------------------------------
// 3. Decoders
// ---------------------------------------------------------------------------

bool parquet_snappy_decompress(const uint8_t *input,
                               const size_t input_size,
                               const size_t expected_size,
                               std::vector<uint8_t> &output,
                               std::string &error)
{
  size_t pos = 0;
  uint64_t declared_size = 0;
  int shift = 0;
  for (;;) {
    if (pos >= input_size) {
      error = "truncated snappy length";
      return false;
    }
    const uint8_t byte = input[pos++];
    declared_size |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if (0 == (byte & 0x80)) {
      break;
    }
    shift += 7;
    if (shift > 35) {
      error = "invalid snappy length";
      return false;
    }
  }
  if (declared_size != expected_size) {
    error = "snappy length mismatch";
    return false;
  }
  output.clear();
  output.reserve(expected_size);

  while (pos < input_size) {
    const uint8_t tag = input[pos++];
    const uint8_t kind = tag & 0x03;
    if (0 == kind) {
      size_t length = static_cast<size_t>(tag >> 2);
      if (length < 60) {
        ++length;
      } else {
        const size_t extra = length - 59;
        if (pos + extra > input_size || extra > 4) {
          error = "truncated snappy literal length";
          return false;
        }
        uint32_t encoded = 0;
        for (size_t i = 0; i < extra; ++i) {
          encoded |= static_cast<uint32_t>(input[pos + i]) << (8 * i);
        }
        pos += extra;
        length = static_cast<size_t>(encoded) + 1;
      }
      if (pos + length > input_size || output.size() + length > expected_size) {
        error = "truncated snappy literal";
        return false;
      }
      output.insert(output.end(), input + pos, input + pos + length);
      pos += length;
    } else {
      size_t length = 0;
      size_t offset = 0;
      if (1 == kind) {
        if (pos >= input_size) {
          error = "truncated snappy copy";
          return false;
        }
        length = 4 + ((tag >> 2) & 0x07);
        offset = (static_cast<size_t>(tag & 0xE0) << 3) | input[pos++];
      } else if (2 == kind) {
        if (pos + 2 > input_size) {
          error = "truncated snappy copy";
          return false;
        }
        length = 1 + (tag >> 2);
        offset = static_cast<size_t>(input[pos]) |
                 (static_cast<size_t>(input[pos + 1]) << 8);
        pos += 2;
      } else {
        if (pos + 4 > input_size) {
          error = "truncated snappy copy";
          return false;
        }
        length = 1 + (tag >> 2);
        offset = static_cast<size_t>(input[pos]) |
                 (static_cast<size_t>(input[pos + 1]) << 8) |
                 (static_cast<size_t>(input[pos + 2]) << 16) |
                 (static_cast<size_t>(input[pos + 3]) << 24);
        pos += 4;
      }
      if (0 == offset || offset > output.size() || output.size() + length > expected_size) {
        error = "invalid snappy copy";
        return false;
      }
      const size_t start = output.size() - offset;
      for (size_t i = 0; i < length; ++i) {
        output.push_back(output[start + i]);
      }
    }
  }

  if (output.size() != expected_size) {
    error = "snappy output size mismatch";
    return false;
  }
  return true;
}

int parquet_bit_width(const uint32_t max_level)
{
  int width = 0;
  while ((1u << width) <= max_level) {
    ++width;
  }
  return width;
}

bool parquet_read_uleb(const uint8_t *data,
                       const size_t size,
                       size_t &pos,
                       uint64_t &value)
{
  value = 0;
  int shift = 0;
  for (;;) {
    if (pos >= size) {
      return false;
    }
    const uint8_t byte = data[pos++];
    value |= static_cast<uint64_t>(byte & 0x7F) << shift;
    if (0 == (byte & 0x80)) {
      return true;
    }
    shift += 7;
    if (shift > 63) {
      return false;
    }
  }
}

/**
 * Decode a Parquet RLE / bit-packed hybrid run.  Returns exactly `num_values`
 * level/index values (extra values in the last run are ignored).
 */
bool parquet_decode_rle_hybrid(const uint8_t *data,
                               const size_t size,
                               const int bit_width,
                               const size_t num_values,
                               std::vector<uint32_t> &values,
                               std::string &error)
{
  values.clear();
  values.reserve(num_values);
  if (num_values == 0) {
    return true;
  }
  if (bit_width < 0 || bit_width > 32) {
    error = "invalid RLE bit width";
    return false;
  }
  size_t pos = 0;
  while (values.size() < num_values) {
    uint64_t header = 0;
    if (!parquet_read_uleb(data, size, pos, header)) {
      error = "truncated RLE header";
      return false;
    }
    if (0 != (header & 1)) {
      const uint64_t groups = header >> 1;
      if (0 == groups) {
        error = "zero-length bit-packed run";
        return false;
      }
      const size_t count = static_cast<size_t>(groups) * 8;
      const size_t bytes = static_cast<size_t>(groups) * static_cast<size_t>(bit_width);
      if (pos + bytes > size) {
        error = "truncated bit-packed run";
        return false;
      }
      for (size_t i = 0; i < count && values.size() < num_values; ++i) {
        uint32_t value = 0;
        for (int bit = 0; bit < bit_width; ++bit) {
          const size_t bit_pos = i * static_cast<size_t>(bit_width) + static_cast<size_t>(bit);
          if (0 != (data[pos + bit_pos / 8] & (1u << (bit_pos % 8)))) {
            value |= (1u << bit);
          }
        }
        values.push_back(value);
      }
      pos += bytes;
    } else {
      const uint64_t run = header >> 1;
      if (0 == run) {
        error = "zero-length RLE run";
        return false;
      }
      const size_t byte_width = static_cast<size_t>((bit_width + 7) / 8);
      if (pos + byte_width > size) {
        error = "truncated RLE value";
        return false;
      }
      uint32_t value = 0;
      for (size_t i = 0; i < byte_width; ++i) {
        value |= static_cast<uint32_t>(data[pos + i]) << (8 * i);
      }
      pos += byte_width;
      for (uint64_t i = 0; i < run && values.size() < num_values; ++i) {
        values.push_back(value);
      }
    }
  }
  return true;
}

void parquet_civil_from_days(int64_t days, int64_t &year, int &month, int &day)
{
  days += 719468;
  const int64_t era = (days >= 0 ? days : days - 146096) / 146097;
  const unsigned day_of_era = static_cast<unsigned>(days - era * 146097);
  const unsigned year_of_era =
      (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
  year = static_cast<int64_t>(year_of_era) + era * 400;
  const unsigned day_of_year =
      day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
  const unsigned month_prime = (5 * day_of_year + 2) / 153;
  day = static_cast<int>(day_of_year - (153 * month_prime + 2) / 5 + 1);
  month = static_cast<int>(month_prime + (month_prime < 10 ? 3 : -9));
  year += (month <= 2 ? 1 : 0);
}

std::string parquet_format_date(const int32_t days_since_epoch)
{
  int64_t year = 0;
  int month = 0;
  int day = 0;
  parquet_civil_from_days(days_since_epoch, year, month, day);
  char buffer[32];
  std::snprintf(buffer, sizeof(buffer), "%04lld-%02d-%02d",
                static_cast<long long>(year), month, day);
  return std::string(buffer);
}

std::string parquet_format_timestamp(const int64_t micros_since_epoch)
{
  int64_t micros = micros_since_epoch;
  int64_t seconds = micros / 1000000;
  int64_t fraction = micros % 1000000;
  if (fraction < 0) {
    fraction += 1000000;
    --seconds;
  }
  const int64_t days = seconds >= 0 ? seconds / 86400 : (seconds - 86399) / 86400;
  int64_t second_of_day = seconds - days * 86400;
  int64_t year = 0;
  int month = 0;
  int day = 0;
  parquet_civil_from_days(days, year, month, day);
  const int hour = static_cast<int>(second_of_day / 3600);
  const int minute = static_cast<int>((second_of_day % 3600) / 60);
  const int second = static_cast<int>(second_of_day % 60);
  char buffer[48];
  if (fraction != 0) {
    std::snprintf(buffer, sizeof(buffer), "%04lld-%02d-%02d %02d:%02d:%02d.%06lld",
                  static_cast<long long>(year), month, day, hour, minute, second,
                  static_cast<long long>(fraction));
  } else {
    std::snprintf(buffer, sizeof(buffer), "%04lld-%02d-%02d %02d:%02d:%02d",
                  static_cast<long long>(year), month, day, hour, minute, second);
  }
  return std::string(buffer);
}

std::string parquet_format_int96(const uint8_t *data)
{
  int64_t nanos_of_day = 0;
  for (int i = 0; i < 8; ++i) {
    nanos_of_day |= static_cast<int64_t>(data[i]) << (8 * i);
  }
  int32_t julian_day = 0;
  for (int i = 0; i < 4; ++i) {
    julian_day |= static_cast<int32_t>(data[8 + i]) << (8 * i);
  }
  // Julian day 2440588 == 1970-01-01.
  const int64_t days = static_cast<int64_t>(julian_day) - 2440588;
  const int64_t micros = days * 86400LL * 1000000LL + nanos_of_day / 1000;
  return parquet_format_timestamp(micros);
}

std::string parquet_format_double(const double value)
{
  char buffer[64];
  const std::to_chars_result result =
      std::to_chars(buffer, buffer + sizeof(buffer), value);
  if (result.ec == std::errc()) {
    return std::string(buffer, result.ptr);
  }
  char fallback[64];
  std::snprintf(fallback, sizeof(fallback), "%.17g", value);
  return std::string(fallback);
}

std::string parquet_format_float(const float value)
{
  char buffer[64];
  const std::to_chars_result result =
      std::to_chars(buffer, buffer + sizeof(buffer), value);
  if (result.ec == std::errc()) {
    return std::string(buffer, result.ptr);
  }
  char fallback[64];
  std::snprintf(fallback, sizeof(fallback), "%.9g", static_cast<double>(value));
  return std::string(fallback);
}

bool parquet_decode_plain_values(const uint8_t *data,
                                 const size_t size,
                                 const int32_t physical_type,
                                 const int32_t converted_type,
                                 const int32_t type_length,
                                 const size_t count,
                                 std::vector<std::string> &values,
                                 std::string &error)
{
  values.clear();
  values.reserve(count);
  size_t pos = 0;
  for (size_t i = 0; i < count; ++i) {
    switch (physical_type) {
      case PARQUET_TYPE_BOOLEAN: {
        const size_t byte_pos = i / 8;
        if (byte_pos >= size) {
          error = "truncated PLAIN boolean";
          return false;
        }
        const bool value = 0 != (data[byte_pos] & (1u << (i % 8)));
        values.push_back(value ? "true" : "false");
        break;
      }
      case PARQUET_TYPE_INT32: {
        if (pos + 4 > size) {
          error = "truncated PLAIN int32";
          return false;
        }
        int32_t value = 0;
        std::memcpy(&value, data + pos, 4);
        pos += 4;
        if (PARQUET_CONVERTED_DATE == converted_type) {
          values.push_back(parquet_format_date(value));
        } else {
          values.push_back(std::to_string(value));
        }
        break;
      }
      case PARQUET_TYPE_INT64: {
        if (pos + 8 > size) {
          error = "truncated PLAIN int64";
          return false;
        }
        int64_t value = 0;
        std::memcpy(&value, data + pos, 8);
        pos += 8;
        if (PARQUET_CONVERTED_TIMESTAMP_MILLIS == converted_type) {
          values.push_back(parquet_format_timestamp(value * 1000));
        } else if (PARQUET_CONVERTED_TIMESTAMP_MICROS == converted_type) {
          values.push_back(parquet_format_timestamp(value));
        } else {
          values.push_back(std::to_string(value));
        }
        break;
      }
      case PARQUET_TYPE_INT96: {
        if (pos + 12 > size) {
          error = "truncated PLAIN int96";
          return false;
        }
        values.push_back(parquet_format_int96(data + pos));
        pos += 12;
        break;
      }
      case PARQUET_TYPE_FLOAT: {
        if (pos + 4 > size) {
          error = "truncated PLAIN float";
          return false;
        }
        float value = 0;
        std::memcpy(&value, data + pos, 4);
        pos += 4;
        values.push_back(parquet_format_float(value));
        break;
      }
      case PARQUET_TYPE_DOUBLE: {
        if (pos + 8 > size) {
          error = "truncated PLAIN double";
          return false;
        }
        double value = 0;
        std::memcpy(&value, data + pos, 8);
        pos += 8;
        values.push_back(parquet_format_double(value));
        break;
      }
      case PARQUET_TYPE_BYTE_ARRAY: {
        if (pos + 4 > size) {
          error = "truncated PLAIN byte array length";
          return false;
        }
        uint32_t length = 0;
        for (int b = 0; b < 4; ++b) {
          length |= static_cast<uint32_t>(data[pos + b]) << (8 * b);
        }
        pos += 4;
        if (pos + length > size) {
          error = "truncated PLAIN byte array";
          return false;
        }
        values.push_back(std::string(reinterpret_cast<const char *>(data + pos), length));
        pos += length;
        break;
      }
      case PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY: {
        if (type_length < 0 || pos + static_cast<size_t>(type_length) > size) {
          error = "truncated PLAIN fixed byte array";
          return false;
        }
        values.push_back(std::string(reinterpret_cast<const char *>(data + pos),
                                     static_cast<size_t>(type_length)));
        pos += static_cast<size_t>(type_length);
        break;
      }
      default:
        error = "unsupported Parquet physical type";
        return false;
    }
  }
  return true;
}

bool parquet_decode_dictionary_indices(const uint8_t *data,
                                       const size_t size,
                                       const size_t count,
                                       const std::vector<std::string> &dictionary,
                                       std::vector<std::string> &values,
                                       std::string &error)
{
  values.clear();
  values.reserve(count);
  if (count == 0) {
    return true;
  }
  if (size < 1) {
    error = "truncated dictionary indices";
    return false;
  }
  const int bit_width = data[0];
  std::vector<uint32_t> indices;
  if (!parquet_decode_rle_hybrid(data + 1, size - 1, bit_width, count, indices, error)) {
    return false;
  }
  for (size_t i = 0; i < indices.size(); ++i) {
    if (indices[i] >= dictionary.size()) {
      error = "dictionary index out of range";
      return false;
    }
    values.push_back(dictionary[indices[i]]);
  }
  return true;
}

} // namespace

// ---------------------------------------------------------------------------
// 4. Reader implementation
// ---------------------------------------------------------------------------

struct ObParquetReader::Impl
{
  struct SchemaColumn
  {
    std::string name;
    int32_t physical_type = -1;
    int32_t converted_type = -1;
    int32_t repetition = -1;
    int32_t type_length = 0;
    ObFileSqlType sql_type = ObFileSqlType::VARCHAR;
    bool nullable = false;
    int32_t max_definition_level = 0;
  };

  ~Impl()
  {
    close();
  }

  void close()
  {
    file.clear();
    meta = ParquetFileMeta();
    schema.clear();
    columns.clear();
    column_values.clear();
    projected.clear();
    options = ObParquetOptions();
    num_rows = 0;
    row_pos = 0;
    opened = false;
    schema_inferred = false;
    eof = false;
    error = ObParquetReadError::NONE;
    error_message.clear();
  }

  void set_error(const ObParquetReadError code, const std::string &message)
  {
    error = code;
    error_message = message;
  }

  bool load_file(const std::string &path)
  {
    std::ifstream stream(path.c_str(), std::ios::in | std::ios::binary);
    if (!stream.is_open()) {
      set_error(ObParquetReadError::OPEN, "failed to open the Parquet file");
      return false;
    }
    stream.seekg(0, std::ios::end);
    const std::streamoff length = stream.tellg();
    if (length < 0) {
      set_error(ObParquetReadError::IO, "failed to stat the Parquet file");
      return false;
    }
    stream.seekg(0, std::ios::beg);
    file.resize(static_cast<size_t>(length));
    if (!file.empty()) {
      stream.read(reinterpret_cast<char *>(file.data()), length);
      if (!stream) {
        set_error(ObParquetReadError::IO, "failed to read the Parquet file");
        return false;
      }
    }
    return true;
  }

  bool parse_footer()
  {
    if (file.size() < 12) {
      set_error(ObParquetReadError::FORMAT, "Parquet file is too small");
      return false;
    }
    if (0 != std::memcmp(file.data(), "PAR1", 4) ||
        0 != std::memcmp(file.data() + file.size() - 4, "PAR1", 4)) {
      set_error(ObParquetReadError::FORMAT, "missing Parquet magic");
      return false;
    }
    const uint8_t *tail = file.data() + file.size() - 8;
    uint32_t footer_length = 0;
    for (int i = 0; i < 4; ++i) {
      footer_length |= static_cast<uint32_t>(tail[i]) << (8 * i);
    }
    if (footer_length > file.size() - 8 || footer_length < 4) {
      set_error(ObParquetReadError::FORMAT, "invalid Parquet footer length");
      return false;
    }
    const size_t footer_start = file.size() - 8 - footer_length;
    ParquetCompactReader reader(file.data(), file.size(), footer_start);
    if (!parquet_parse_file_meta(reader, meta)) {
      set_error(ObParquetReadError::FORMAT, "failed to parse the Parquet footer");
      return false;
    }
    if (meta.schema.empty()) {
      set_error(ObParquetReadError::FORMAT, "Parquet footer has no schema");
      return false;
    }
    return true;
  }

  ObFileSqlType map_sql_type(const ParquetSchemaElement &element) const
  {
    if (PARQUET_TYPE_BOOLEAN == element.type) {
      return ObFileSqlType::BOOLEAN;
    }
    if (PARQUET_TYPE_INT32 == element.type) {
      if (PARQUET_CONVERTED_DATE == element.converted) {
        return ObFileSqlType::DATE;
      }
      return ObFileSqlType::BIGINT;
    }
    if (PARQUET_TYPE_INT64 == element.type) {
      if (PARQUET_CONVERTED_TIMESTAMP_MILLIS == element.converted ||
          PARQUET_CONVERTED_TIMESTAMP_MICROS == element.converted) {
        return ObFileSqlType::DATETIME;
      }
      return ObFileSqlType::BIGINT;
    }
    if (PARQUET_TYPE_INT96 == element.type) {
      return ObFileSqlType::DATETIME;
    }
    if (PARQUET_TYPE_FLOAT == element.type || PARQUET_TYPE_DOUBLE == element.type) {
      return ObFileSqlType::DOUBLE;
    }
    if (PARQUET_TYPE_BYTE_ARRAY == element.type ||
        PARQUET_TYPE_FIXED_LEN_BYTE_ARRAY == element.type) {
      return ObFileSqlType::VARCHAR;
    }
    return ObFileSqlType::VARCHAR;
  }

  bool build_schema()
  {
    const ParquetSchemaElement &root = meta.schema[0];
    int32_t child_count = root.num_children;
    if (child_count <= 0) {
      set_error(ObParquetReadError::FORMAT, "Parquet schema has no columns");
      return false;
    }
    size_t index = 1;
    for (int32_t i = 0; i < child_count; ++i) {
      if (index >= meta.schema.size()) {
        set_error(ObParquetReadError::FORMAT, "truncated Parquet schema");
        return false;
      }
      const ParquetSchemaElement &element = meta.schema[index++];
      if (element.num_children > 0) {
        set_error(ObParquetReadError::UNSUPPORTED,
                  "nested Parquet schema is not supported");
        return false;
      }
      if (PARQUET_REPETITION_REPEATED == element.repetition) {
        set_error(ObParquetReadError::UNSUPPORTED,
                  "repeated Parquet columns are not supported");
        return false;
      }
      SchemaColumn column;
      column.name = element.name;
      column.physical_type = element.type;
      column.converted_type = element.converted;
      column.repetition = element.repetition;
      column.type_length = element.type_length;
      column.sql_type = map_sql_type(element);
      column.nullable = (PARQUET_REPETITION_OPTIONAL == element.repetition);
      column.max_definition_level = column.nullable ? 1 : 0;
      schema.push_back(std::move(column));
    }
    if (index != meta.schema.size()) {
      set_error(ObParquetReadError::UNSUPPORTED,
                "nested Parquet schema is not supported");
      return false;
    }
    return true;
  }

  bool is_projected(const std::string &name) const
  {
    if (projected.empty()) {
      return true;
    }
    return projected.count(name) > 0;
  }

  const ParquetColumnChunk *find_chunk(const ParquetRowGroup &group,
                                       const std::string &name) const
  {
    for (size_t i = 0; i < group.columns.size(); ++i) {
      const ParquetColumnChunk &chunk = group.columns[i];
      if (!chunk.has_meta) {
        continue;
      }
      if (chunk.meta.path.size() != 1) {
        continue;
      }
      if (chunk.meta.path[0] == name) {
        return &chunk;
      }
    }
    return nullptr;
  }

  bool decompress_page(const int32_t codec,
                       const uint8_t *data,
                       const size_t size,
                       const size_t expected_size,
                       std::vector<uint8_t> &buffer,
                       const uint8_t *&out,
                       size_t &out_size)
  {
    if (PARQUET_CODEC_UNCOMPRESSED == codec) {
      out = data;
      out_size = size;
      return true;
    }
    if (PARQUET_CODEC_SNAPPY != codec) {
      set_error(ObParquetReadError::UNSUPPORTED,
                "unsupported Parquet compression codec");
      return false;
    }
    std::string message;
    if (!parquet_snappy_decompress(data, size, expected_size, buffer, message)) {
      set_error(ObParquetReadError::FORMAT, "snappy decompression failed: " + message);
      return false;
    }
    out = buffer.data();
    out_size = buffer.size();
    return true;
  }

  bool decode_data_page(const SchemaColumn &column,
                        const ParquetPageHeader &header,
                        const uint8_t *body,
                        const size_t body_size,
                        const std::vector<std::string> &dictionary,
                        std::vector<std::string> &out)
  {
    if (!header.has_data_page) {
      set_error(ObParquetReadError::FORMAT, "data page without a V1 header");
      return false;
    }
    const ParquetDataPageHeader &page = header.data_page;
    if (page.num_values < 0) {
      set_error(ObParquetReadError::FORMAT, "negative data page value count");
      return false;
    }
    std::vector<uint8_t> buffer;
    const uint8_t *page_data = nullptr;
    size_t page_size = 0;
    if (!decompress_page(column_codec, body, body_size,
                         static_cast<size_t>(header.uncompressed_page_size),
                         buffer, page_data, page_size)) {
      return false;
    }

    const size_t num_values = static_cast<size_t>(page.num_values);
    size_t pos = 0;
    std::vector<uint32_t> definition_levels;
    if (column.max_definition_level > 0) {
      if (PARQUET_ENCODING_RLE != page.definition_level_encoding &&
          PARQUET_ENCODING_BIT_PACKED != page.definition_level_encoding) {
        set_error(ObParquetReadError::UNSUPPORTED,
                  "unsupported definition level encoding");
        return false;
      }
      if (pos + 4 > page_size) {
        set_error(ObParquetReadError::FORMAT, "truncated definition level length");
        return false;
      }
      uint32_t level_length = 0;
      for (int i = 0; i < 4; ++i) {
        level_length |= static_cast<uint32_t>(page_data[pos + i]) << (8 * i);
      }
      pos += 4;
      if (pos + level_length > page_size) {
        set_error(ObParquetReadError::FORMAT, "truncated definition levels");
        return false;
      }
      std::string message;
      const int bit_width = parquet_bit_width(static_cast<uint32_t>(column.max_definition_level));
      if (PARQUET_ENCODING_BIT_PACKED == page.definition_level_encoding) {
        set_error(ObParquetReadError::UNSUPPORTED,
                  "BIT_PACKED definition levels are not supported");
        return false;
      }
      if (!parquet_decode_rle_hybrid(page_data + pos, level_length, bit_width,
                                     num_values, definition_levels, message)) {
        set_error(ObParquetReadError::FORMAT,
                  "failed to decode definition levels: " + message);
        return false;
      }
      pos += level_length;
    } else {
      definition_levels.assign(num_values, 1);
    }

    size_t present_count = 0;
    for (size_t i = 0; i < definition_levels.size(); ++i) {
      if (definition_levels[i] >= static_cast<uint32_t>(column.max_definition_level)) {
        ++present_count;
      }
    }

    std::vector<std::string> values;
    if (num_values > 0) {
      if (PARQUET_ENCODING_PLAIN == page.encoding) {
        std::string message;
        if (!parquet_decode_plain_values(page_data + pos, page_size - pos,
                                         column.physical_type, column.converted_type,
                                         column.type_length, present_count,
                                         values, message)) {
          set_error(ObParquetReadError::FORMAT, "failed to decode PLAIN values: " + message);
          return false;
        }
      } else if (PARQUET_ENCODING_PLAIN_DICTIONARY == page.encoding ||
                 PARQUET_ENCODING_RLE_DICTIONARY == page.encoding) {
        if (present_count > 0 && dictionary.empty()) {
          set_error(ObParquetReadError::FORMAT, "dictionary page is missing");
          return false;
        }
        std::string message;
        if (!parquet_decode_dictionary_indices(page_data + pos, page_size - pos,
                                               present_count, dictionary, values, message)) {
          set_error(ObParquetReadError::FORMAT,
                    "failed to decode dictionary values: " + message);
          return false;
        }
      } else {
        set_error(ObParquetReadError::UNSUPPORTED,
                  "unsupported Parquet value encoding");
        return false;
      }
    }

    size_t value_index = 0;
    for (size_t i = 0; i < num_values; ++i) {
      if (definition_levels[i] >= static_cast<uint32_t>(column.max_definition_level)) {
        if (value_index >= values.size()) {
          set_error(ObParquetReadError::FORMAT, "too few decoded values");
          return false;
        }
        out.push_back(values[value_index++]);
      } else {
        out.push_back(std::string());
      }
    }
    return true;
  }

  bool decode_data_page_v2(const SchemaColumn &column,
                           const ParquetPageHeader &header,
                           const uint8_t *body,
                           const size_t body_size,
                           const std::vector<std::string> &dictionary,
                           std::vector<std::string> &out)
  {
    if (!header.has_data_page_v2) {
      set_error(ObParquetReadError::FORMAT, "data page without a V2 header");
      return false;
    }
    const ParquetDataPageHeaderV2 &page = header.data_page_v2;
    if (page.num_values < 0 || page.definition_levels_byte_length < 0 ||
        page.repetition_levels_byte_length < 0) {
      set_error(ObParquetReadError::FORMAT, "invalid V2 page lengths");
      return false;
    }
    const size_t rep_length = static_cast<size_t>(page.repetition_levels_byte_length);
    const size_t def_length = static_cast<size_t>(page.definition_levels_byte_length);
    if (rep_length + def_length > body_size) {
      set_error(ObParquetReadError::FORMAT, "truncated V2 level bytes");
      return false;
    }

    const size_t num_values = static_cast<size_t>(page.num_values);
    const uint8_t *level_data = body;
    const uint8_t *value_data = body + rep_length + def_length;
    const size_t compressed_values_size = body_size - rep_length - def_length;
    const size_t uncompressed_values_size =
        static_cast<size_t>(header.uncompressed_page_size) - rep_length - def_length;
    if (static_cast<int64_t>(rep_length + def_length) > header.uncompressed_page_size) {
      set_error(ObParquetReadError::FORMAT, "invalid V2 page sizes");
      return false;
    }

    std::vector<uint32_t> definition_levels;
    if (column.max_definition_level > 0) {
      std::string message;
      const int bit_width = parquet_bit_width(static_cast<uint32_t>(column.max_definition_level));
      if (!parquet_decode_rle_hybrid(level_data + rep_length, def_length, bit_width,
                                     num_values, definition_levels, message)) {
        set_error(ObParquetReadError::FORMAT,
                  "failed to decode V2 definition levels: " + message);
        return false;
      }
    } else {
      definition_levels.assign(num_values, 1);
    }

    size_t present_count = 0;
    for (size_t i = 0; i < definition_levels.size(); ++i) {
      if (definition_levels[i] >= static_cast<uint32_t>(column.max_definition_level)) {
        ++present_count;
      }
    }

    std::vector<uint8_t> buffer;
    const uint8_t *values_data = value_data;
    size_t values_size = compressed_values_size;
    if (page.is_compressed && PARQUET_CODEC_UNCOMPRESSED != column_codec) {
      if (!decompress_page(column_codec, value_data, compressed_values_size,
                           uncompressed_values_size, buffer, values_data, values_size)) {
        return false;
      }
    }

    std::vector<std::string> values;
    if (num_values > 0) {
      if (PARQUET_ENCODING_PLAIN == page.encoding) {
        std::string message;
        if (!parquet_decode_plain_values(values_data, values_size,
                                         column.physical_type, column.converted_type,
                                         column.type_length, present_count,
                                         values, message)) {
          set_error(ObParquetReadError::FORMAT, "failed to decode PLAIN values: " + message);
          return false;
        }
      } else if (PARQUET_ENCODING_PLAIN_DICTIONARY == page.encoding ||
                 PARQUET_ENCODING_RLE_DICTIONARY == page.encoding) {
        if (present_count > 0 && dictionary.empty()) {
          set_error(ObParquetReadError::FORMAT, "dictionary page is missing");
          return false;
        }
        std::string message;
        if (!parquet_decode_dictionary_indices(values_data, values_size,
                                               present_count, dictionary, values, message)) {
          set_error(ObParquetReadError::FORMAT,
                    "failed to decode dictionary values: " + message);
          return false;
        }
      } else {
        set_error(ObParquetReadError::UNSUPPORTED,
                  "unsupported Parquet value encoding");
        return false;
      }
    }

    size_t value_index = 0;
    for (size_t i = 0; i < num_values; ++i) {
      if (definition_levels[i] >= static_cast<uint32_t>(column.max_definition_level)) {
        if (value_index >= values.size()) {
          set_error(ObParquetReadError::FORMAT, "too few decoded values");
          return false;
        }
        out.push_back(values[value_index++]);
      } else {
        out.push_back(std::string());
      }
    }
    return true;
  }

  bool decode_column_chunk(const SchemaColumn &column,
                           const ParquetColumnChunk &chunk,
                           const int64_t expected_rows,
                           std::vector<std::string> &out)
  {
    out.clear();
    out.reserve(static_cast<size_t>(expected_rows));
    if (!chunk.has_meta) {
      set_error(ObParquetReadError::FORMAT, "column chunk has no metadata");
      return false;
    }
    const ParquetColumnMeta &meta_data = chunk.meta;
    if (!chunk.file_path.empty()) {
      set_error(ObParquetReadError::UNSUPPORTED,
                "external Parquet column chunks are not supported");
      return false;
    }
    if (meta_data.type != column.physical_type) {
      set_error(ObParquetReadError::FORMAT, "column chunk type mismatch");
      return false;
    }
    if (PARQUET_CODEC_UNCOMPRESSED != meta_data.codec &&
        PARQUET_CODEC_SNAPPY != meta_data.codec) {
      set_error(ObParquetReadError::UNSUPPORTED,
                "unsupported Parquet compression codec");
      return false;
    }
    int64_t start = meta_data.data_page_offset;
    if (meta_data.dictionary_page_offset >= 0 &&
        meta_data.dictionary_page_offset < start) {
      start = meta_data.dictionary_page_offset;
    }
    if (start < 0 || static_cast<uint64_t>(start) >= file.size()) {
      set_error(ObParquetReadError::FORMAT, "invalid Parquet column offset");
      return false;
    }

    column_codec = meta_data.codec;
    std::vector<std::string> dictionary;
    size_t pos = static_cast<size_t>(start);
    int64_t collected = 0;
    while (collected < expected_rows) {
      if (pos >= file.size()) {
        set_error(ObParquetReadError::FORMAT, "truncated Parquet column chunk");
        return false;
      }
      ParquetCompactReader reader(file.data(), file.size(), pos);
      ParquetPageHeader header;
      if (!parquet_parse_page_header(reader, header)) {
        set_error(ObParquetReadError::FORMAT, "failed to parse a Parquet page header");
        return false;
      }
      const size_t header_end = reader.pos();
      if (header.compressed_page_size < 0 ||
          header_end + static_cast<size_t>(header.compressed_page_size) > file.size()) {
        set_error(ObParquetReadError::FORMAT, "truncated Parquet page body");
        return false;
      }
      const uint8_t *body = file.data() + header_end;
      const size_t body_size = static_cast<size_t>(header.compressed_page_size);
      if (PARQUET_PAGE_DICTIONARY == header.type) {
        if (!header.has_dictionary_page) {
          set_error(ObParquetReadError::FORMAT, "dictionary page without a header");
          return false;
        }
        std::vector<uint8_t> buffer;
        const uint8_t *page_data = nullptr;
        size_t page_size = 0;
        if (!decompress_page(column_codec, body, body_size,
                             static_cast<size_t>(header.uncompressed_page_size),
                             buffer, page_data, page_size)) {
          return false;
        }
        std::string message;
        if (!parquet_decode_plain_values(
                page_data, page_size, column.physical_type, column.converted_type,
                column.type_length,
                static_cast<size_t>(header.dictionary_page.num_values),
                dictionary, message)) {
          set_error(ObParquetReadError::FORMAT,
                    "failed to decode the dictionary page: " + message);
          return false;
        }
      } else if (PARQUET_PAGE_DATA == header.type) {
        const int64_t before = static_cast<int64_t>(out.size());
        if (!decode_data_page(column, header, body, body_size, dictionary, out)) {
          return false;
        }
        collected += static_cast<int64_t>(out.size()) - before;
      } else if (PARQUET_PAGE_DATA_V2 == header.type) {
        const int64_t before = static_cast<int64_t>(out.size());
        if (!decode_data_page_v2(column, header, body, body_size, dictionary, out)) {
          return false;
        }
        collected += static_cast<int64_t>(out.size()) - before;
      } else {
        // INDEX_PAGE and unknown page types carry no row values.
      }
      if (0 == header.compressed_page_size) {
        set_error(ObParquetReadError::FORMAT, "zero-sized Parquet page");
        return false;
      }
      pos = header_end + body_size;
    }
    if (static_cast<int64_t>(out.size()) != expected_rows) {
      set_error(ObParquetReadError::FORMAT, "column chunk row count mismatch");
      return false;
    }
    return true;
  }

  bool decode_all()
  {
    column_values.assign(schema.size(), std::vector<std::string>());
    projected.clear();
    for (size_t i = 0; i < options.projected_columns.size(); ++i) {
      bool found = false;
      for (size_t c = 0; c < schema.size(); ++c) {
        if (schema[c].name == options.projected_columns[i]) {
          found = true;
          break;
        }
      }
      if (!found) {
        set_error(ObParquetReadError::UNSUPPORTED,
                  "unknown projected Parquet column: " + options.projected_columns[i]);
        return false;
      }
      projected.insert(options.projected_columns[i]);
    }

    int64_t total_rows = 0;
    for (size_t rg = 0; rg < meta.row_groups.size(); ++rg) {
      const ParquetRowGroup &group = meta.row_groups[rg];
      for (size_t c = 0; c < schema.size(); ++c) {
        if (!is_projected(schema[c].name)) {
          continue;
        }
        const ParquetColumnChunk *chunk = find_chunk(group, schema[c].name);
        if (nullptr == chunk) {
          set_error(ObParquetReadError::FORMAT,
                    "missing column chunk for " + schema[c].name);
          return false;
        }
        std::vector<std::string> values;
        if (!decode_column_chunk(schema[c], *chunk, group.num_rows, values)) {
          return false;
        }
        column_values[c].insert(column_values[c].end(),
                                std::make_move_iterator(values.begin()),
                                std::make_move_iterator(values.end()));
      }
      total_rows += group.num_rows;
    }
    for (size_t c = 0; c < schema.size(); ++c) {
      if (is_projected(schema[c].name) &&
          static_cast<int64_t>(column_values[c].size()) != total_rows) {
        set_error(ObParquetReadError::FORMAT, "decoded row count mismatch");
        return false;
      }
    }
    if (meta.num_rows != total_rows) {
      set_error(ObParquetReadError::FORMAT, "footer row count mismatch");
      return false;
    }
    num_rows = total_rows;
    return true;
  }

  std::vector<uint8_t> file;
  ParquetFileMeta meta;
  std::vector<SchemaColumn> schema;
  std::vector<Column> columns;
  std::vector<std::vector<std::string>> column_values;
  std::set<std::string> projected;
  ObParquetOptions options;
  int32_t column_codec = PARQUET_CODEC_UNCOMPRESSED;
  int64_t num_rows = 0;
  size_t row_pos = 0;
  bool opened = false;
  bool schema_inferred = false;
  bool eof = false;
  ObParquetReadError error = ObParquetReadError::NONE;
  std::string error_message;
};

ObParquetReader::ObParquetReader() : impl_(new Impl())
{
}

ObParquetReader::~ObParquetReader() = default;

int ObParquetReader::open(const std::string &path, const ObParquetOptions &options)
{
  if (!impl_) {
    impl_.reset(new Impl());
  }
  impl_->close();
  impl_->options = options;
  if (!impl_->load_file(path)) {
    return -1;
  }
  if (!impl_->parse_footer()) {
    return -1;
  }
  impl_->opened = true;
  return 0;
}

void ObParquetReader::close()
{
  if (impl_) {
    impl_->close();
  }
}

bool ObParquetReader::is_open() const
{
  return impl_ && impl_->opened;
}

int ObParquetReader::infer_schema()
{
  if (!impl_ || !impl_->opened) {
    return -1;
  }
  Impl &state = *impl_;
  state.schema.clear();
  state.columns.clear();
  state.column_values.clear();
  state.num_rows = 0;
  state.row_pos = 0;
  state.schema_inferred = false;
  state.error = ObParquetReadError::NONE;
  state.error_message.clear();
  if (!state.build_schema() || !state.decode_all()) {
    return -1;
  }
  for (size_t i = 0; i < state.schema.size(); ++i) {
    Column column;
    column.name = state.schema[i].name;
    column.type = state.schema[i].sql_type;
    column.nullable = state.schema[i].nullable || state.num_rows == 0;
    state.columns.push_back(std::move(column));
  }
  state.schema_inferred = true;
  return 0;
}

int ObParquetReader::next_batch(const size_t capacity, std::vector<Row> &rows)
{
  rows.clear();
  if (!impl_ || !impl_->opened) {
    return -1;
  }
  if (capacity == 0 || impl_->eof) {
    return 0;
  }
  if (!impl_->schema_inferred) {
    impl_->set_error(ObParquetReadError::FORMAT, "infer_schema() must be called first");
    return -1;
  }
  rows.reserve(capacity);
  while (rows.size() < capacity && impl_->row_pos < static_cast<size_t>(impl_->num_rows)) {
    Row row;
    row.fields.resize(impl_->columns.size());
    for (size_t c = 0; c < impl_->schema.size(); ++c) {
      if (impl_->is_projected(impl_->schema[c].name)) {
        row.fields[c] = impl_->column_values[c][impl_->row_pos];
      }
    }
    rows.push_back(std::move(row));
    ++impl_->row_pos;
  }
  if (impl_->row_pos >= static_cast<size_t>(impl_->num_rows)) {
    impl_->eof = true;
  }
  return 0;
}

int ObParquetReader::rewind()
{
  if (!impl_ || !impl_->opened) {
    return -1;
  }
  impl_->row_pos = 0;
  impl_->eof = false;
  return 0;
}

const std::vector<Column> &ObParquetReader::columns() const
{
  static const std::vector<Column> empty_columns;
  return impl_ ? impl_->columns : empty_columns;
}

const ObParquetOptions &ObParquetReader::options() const
{
  static const ObParquetOptions default_options;
  return impl_ ? impl_->options : default_options;
}

bool ObParquetReader::eof() const
{
  return impl_ && impl_->eof;
}

int64_t ObParquetReader::scanned_row_count() const
{
  return impl_ ? impl_->num_rows : 0;
}

ObParquetReadError ObParquetReader::last_error() const
{
  return impl_ ? impl_->error : ObParquetReadError::NONE;
}

const std::string &ObParquetReader::last_error_message() const
{
  static const std::string empty_message;
  return impl_ ? impl_->error_message : empty_message;
}

} // namespace storage
} // namespace oceanbase
