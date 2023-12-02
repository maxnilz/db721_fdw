#include "reader.h"

#include <sstream>
#include <stdexcept>
#include <utility>

extern "C" {
static void ObjectFieldStartHandler(void *pstate, char *fname, [[maybe_unused]] bool isnull) {
  auto metadata = reinterpret_cast<Metadata *>(pstate);
  if (metadata->parse_stack_ == nullptr) {
    metadata->stack_size_ = BASE_SIZE;
    metadata->parse_stack_ = (char **)palloc0(sizeof(char *) * metadata->stack_size_);
  }
  if (metadata->stack_top_ == metadata->stack_size_) {
    auto size = metadata->stack_size_;
    metadata->stack_size_ *= 2;
    metadata->parse_stack_ = (char **)repalloc(metadata->parse_stack_, sizeof(char *) * metadata->stack_size_);
    memset(metadata->parse_stack_ + size, 0, sizeof(char *) * (metadata->stack_size_ - size));
  }
  metadata->parse_stack_[metadata->stack_top_++] = fname;

  if (metadata->stack_top_ == 2 && strcmp(metadata->parse_stack_[0], "Columns") == 0) {
    if (metadata->columns_ == nullptr) {
      metadata->columns_size_ = BASE_SIZE;
      metadata->columns_ = (Column *)palloc0((sizeof(Column) * metadata->columns_size_));
    }
    if (metadata->column_length_ == metadata->columns_size_) {
      auto size = metadata->columns_size_;
      metadata->columns_size_ *= 2;
      metadata->columns_ = (Column *)repalloc(metadata->columns_, sizeof(Column) * metadata->columns_size_);
      memset(metadata->columns_ + size, 0, sizeof(Column) * (metadata->columns_size_ - size));
    }
    auto &column = metadata->columns_[metadata->column_length_++];
    column.name_ = fname;
    return;
  }

  if (metadata->stack_top_ == 4 && strcmp(metadata->parse_stack_[2], "block_stats") == 0) {
    auto &column = metadata->columns_[metadata->column_length_ - 1];
    Require(column.type_ != BlockType::INVALID, "invalid column type");
    if (column.stats_ == nullptr) {
      column.stats_size_ = BASE_SIZE;
      column.stats_ = (BlockStat *)palloc0(sizeof(BlockStat) * column.stats_size_);
    }
    if (column.stats_length_ == column.stats_size_) {
      auto size = column.stats_size_;
      column.stats_size_ *= 2;
      column.stats_ = (BlockStat *)repalloc(column.stats_, sizeof(BlockStat) * column.stats_size_);
      memset(column.stats_ + size, 0, sizeof(BlockStat) * (column.stats_size_ - size));
    }
    column.stats_[column.stats_length_++].type_ = column.type_;
    return;
  }
}

static void ObjectFieldEndHandler(void *pstate, [[maybe_unused]] char *fname, [[maybe_unused]] bool isnull) {
  auto metadata = reinterpret_cast<Metadata *>(pstate);
  metadata->parse_stack_[--metadata->stack_top_] = nullptr;
}

static void ScalarHandler(void *pstate, char *token, JsonTokenType tokentype) {
  auto metadata = reinterpret_cast<Metadata *>(pstate);
  Require(metadata->stack_top_ > 0, "require keys must be present");
  auto key = metadata->parse_stack_[metadata->stack_top_ - 1];
  switch (tokentype) {
    case JSON_TOKEN_STRING:
      Require(token != nullptr, "require token is not null");
      if (strcmp(key, "Table") == 0) {
        metadata->table_ = token;
        break;
      }
      if (strcmp(key, "type") == 0) {
        Require(metadata->column_length_ > 0, "require columns is not empty");
        auto &column = metadata->columns_[metadata->column_length_ - 1];
        column.type_ = BlockType::INVALID;
        if (strcmp(token, "int") == 0) {
          column.type_ = BlockType::INT;
        } else if (strcmp(token, "str") == 0) {
          column.type_ = BlockType::STR;
        } else if (strcmp(token, "float") == 0) {
          column.type_ = BlockType::FLOAT;
        }
        break;
      }
      if (strcmp(key, "min") == 0) {
        Require(metadata->column_length_ > 0, "require columns is not empty");
        auto &column = metadata->columns_[metadata->column_length_ - 1];
        Require(column.stats_length_ > 0, "require column stats is not empty");
        Require(column.type_ == BlockType::STR, "require column type is str");
        column.stats_[column.stats_length_ - 1].str_.min_ = token;
        break;
      }
      if (strcmp(key, "max") == 0) {
        Require(metadata->column_length_ > 0, "require columns is not empty");
        auto &column = metadata->columns_[metadata->column_length_ - 1];
        Require(column.stats_length_ > 0, "require column stats is not empty");
        Require(column.type_ == BlockType::STR, "require column type is str");
        column.stats_[column.stats_length_ - 1].str_.max_ = token;
        break;
      }
      break;
    case JSON_TOKEN_NUMBER: {
      Require(metadata->column_length_ > 0, "require columns is not empty");
      auto &column = metadata->columns_[metadata->column_length_ - 1];
      if (strcmp(key, "num_blocks") == 0) {
        column.num_blocks_ = std::strtol(token, nullptr, 10);
        break;
      }
      if (strcmp(key, "start_offset") == 0) {
        column.start_offset_ = std::strtol(token, nullptr, 10);
        break;
      }
      if (strcmp(key, "Max Values Per Block") == 0) {
        metadata->max_values_per_block_ = std::strtol(token, nullptr, 10);
        break;
      }
      if (strcmp(key, "num") == 0) {
        Require(column.stats_length_ > 0, "require column stats is not empty");
        column.stats_[column.stats_length_ - 1].num_ = std::strtol(token, nullptr, 10);
        break;
      }
      if (strcmp(key, "min") == 0 || strcmp(key, "max") == 0 || strcmp(key, "min_len") == 0 ||
          strcmp(key, "max_len") == 0) {
        Require(column.stats_length_ > 0, "require column stats is not empty");
        switch (column.type_) {
          case BlockType::INT: {
            auto &is = column.stats_[column.stats_length_ - 1].int_;
            if (strcmp(key, "min") == 0) {
              is.min_ = std::strtol(token, nullptr, 10);
            } else if (strcmp(key, "max") == 0) {
              is.max_ = std::strtol(token, nullptr, 10);
            }
            break;
          }
          case BlockType::FLOAT: {
            auto &fs = column.stats_[column.stats_length_ - 1].float_;
            if (strcmp(key, "min") == 0) {
              fs.min_ = std::strtof(token, nullptr);
            } else if (strcmp(key, "max") == 0) {
              fs.max_ = std::strtof(token, nullptr);
            }
            break;
          }
          case BlockType::STR: {
            auto &ss = column.stats_[column.stats_length_ - 1].str_;
            if (strcmp(key, "min_len") == 0) {
              ss.min_len_ = std::strtol(token, nullptr, 10);
            } else if (strcmp(key, "max_len") == 0) {
              ss.max_len_ = std::strtol(token, nullptr, 10);
            }
          }
          default:
            break;
        }
        break;
      }
      break;
    }
    default:
      break;
  }
}
}

Reader::Reader(std::string name, Metadata *metadata, TupleDesc tuple_desc, std::vector<int16> candidate_blocks,
               ExprState *qual, MemoryContext ectx_str_slab_memory, TupleTableSlot *rel_tuple_slot, List *scan_tlist)
    : name_(std::move(name)),
      metadata_(metadata),
      candidate_blocks_(std::move(candidate_blocks)),
      qual_(qual),
      ectx_str_slab_memory_(ectx_str_slab_memory),
      rel_tuple_slot_(rel_tuple_slot),
      scan_tlist_(scan_tlist) {
  is_ = std::ifstream(metadata->filename_);
  std::unordered_map<std::string, int16> m;
  for (int i = 0; i < tuple_desc->natts; ++i) {
    if (scan_tlist != nullptr) {
      auto tle = static_cast<TargetEntry *>(list_nth(scan_tlist_, i));
      auto var = reinterpret_cast<Var *>(tle->expr);
      auto attname = NameStr(TupleDescAttr(rel_tuple_slot->tts_tupleDescriptor, var->varattno - 1)->attname);
      m[attname] = tuple_desc->attrs[i].attnum - 1;
      continue;
    }
    auto attname = NameStr(tuple_desc->attrs[i].attname);
    m[attname] = tuple_desc->attrs[i].attnum - 1;
  }
  for (auto i = 0; i < metadata->column_length_; ++i) {
    auto &it = metadata->columns_[i];
    columns_map_.emplace(m[it.name_], &it);
  }

  for (int i = 0; i < metadata->num_blocks_; ++i) {
    std::vector<std::shared_ptr<Block>> a(tuple_desc->natts, nullptr);
    blocks_.push_back(a);
  }

  elog(LOG, "scan attrnum: %d", tuple_desc->natts);

  if (!candidate_blocks_.empty()) {
    current_candidate_blocks_idx_ = 0;
    current_block_id_ = candidate_blocks_[current_candidate_blocks_idx_];
    eof_reached_ = false;
  }
}

Reader::~Reader() { is_.close(); }

auto Reader::ParseMetadata(const std::string &filename) -> Metadata * {
  auto is = std::ifstream(filename);
  if (!is.is_open()) {
    std::ostringstream oss;
    oss << "Failed to open file: " << filename;
    throw std::runtime_error(oss.str());
  }

  int32_t metadata_len = 0;
  auto hdr_size = static_cast<int>(sizeof(metadata_len));
  is.seekg(-hdr_size, std::ios::end);  // go to the end of the file
  is.read(reinterpret_cast<char *>(&metadata_len), hdr_size);

  std::string md(metadata_len, '\0');
  is.seekg(-(hdr_size + metadata_len), std::ios::end);
  is.read(md.data(), metadata_len);

  auto lex = makeJsonLexContextCstringLen(md.data(), metadata_len, GetDatabaseEncoding(), true);

  // use palloc to allocate the POD metadata
  // that managed by postgres memory context.
  auto metadata = static_cast<Metadata *>(palloc0(sizeof(Metadata)));
  metadata->filename_ = pstrdup(filename.c_str());

  JsonSemAction sem;
  memset(&sem, 0, sizeof(sem));
  sem.semstate = reinterpret_cast<void *>(metadata);
  sem.object_field_start = ObjectFieldStartHandler;
  sem.object_field_end = ObjectFieldEndHandler;
  sem.scalar = ScalarHandler;

  // pg_parse_json is guaranteed not to generate any error
  // that causes a distant longjmp(), so it's okay to use
  // non POD objects above, i.e., std::string, etc.
  auto result = pg_parse_json(lex, &sem);
  if (result != JSON_SUCCESS) {
    std::ostringstream oss;
    oss << "Found invalid metadata in file: " << filename;
    throw std::runtime_error(oss.str());
  }

  // use first column to populate some helper metrics.
  if (metadata->column_length_ > 0) {
    auto &first_column = metadata->columns_[0];
    metadata->num_blocks_ = first_column.num_blocks_;
    metadata->block_values_ = static_cast<int64 *>(palloc0(sizeof(int64) * metadata->num_blocks_));
    for (auto i = 0; i < first_column.num_blocks_; ++i) {
      metadata->block_values_[i] = first_column.stats_[i].num_;
      metadata->total_rows_ += first_column.stats_[i].num_;
    }
  }

  return metadata;
}

auto Reader::GetColumnByName(const Metadata *md, const std::string &col_name) -> Column * {
  std::unordered_map<std::string, Column *> columns_map;
  for (auto i = 0; i < md->column_length_; ++i) {
    auto &it = md->columns_[i];
    columns_map.emplace(it.name_, &it);
  }
  auto it = columns_map.find(col_name);
  if (it == columns_map.end()) {
    return nullptr;
  }
  return it->second;
}

auto Reader::GetBlock(const int16 attrunm, const int16 block_index) -> Block * {
  if (blocks_[block_index][attrunm] != nullptr) {
    return blocks_[block_index][attrunm].get();
  }
  auto column = columns_map_[attrunm];
  if (column == nullptr || block_index == column->num_blocks_) {
    return nullptr;
  }
  // calculate offset & length
  int64 width = INT_FLOAT_SIZE;
  if (column->type_ == BlockType::STR) {
    width = STR_SIZE;
  }
  int64 skip_num = 0;
  for (int i = 0; i < block_index; i++) {
    skip_num += column->stats_[i].num_;
  }
  auto offset = column->start_offset_ + skip_num * width;
  auto length = column->stats_[block_index].num_ * width;

  // read data
  std::string data(length, 0);
  is_.seekg(offset, std::ios::beg);
  is_.read(data.data(), length);

  auto block = std::make_shared<Block>(data, column, width);
  blocks_[block_index][attrunm] = block;

  return block.get();
}

void Reader::Next(TupleTableSlot *slot, ExprContext *econtext) {
  while (!eof_reached_) {
    Assert(current_block_id_ != INVALID_BLOCK);
    Assert(current_candidate_blocks_idx_ != INVALID_BLOCK_IDX);
    Next(slot);
    if (TupIsNull(slot)) {
      break;
    }
    econtext->ecxt_scantuple = scan_tlist_ == nullptr ? slot : rel_tuple_slot_;
    if (qual_ == nullptr || ExecQual(qual_, econtext)) {
      break;
    }
  }
}

void Reader::Next(TupleTableSlot *slot) {
  ExecClearTuple(slot);
  if (offset_in_block_ == metadata_->block_values_[current_block_id_]) {
    ++current_candidate_blocks_idx_;
    if (current_candidate_blocks_idx_ == static_cast<int16>(candidate_blocks_.size())) {
      eof_reached_ = true;
      return;
    }
    current_block_id_ = candidate_blocks_[current_candidate_blocks_idx_];
    offset_in_block_ = 0;
  }
  auto tuple_desc = slot->tts_tupleDescriptor;
  for (int i = 0; i < tuple_desc->natts; ++i) {
    auto attr = tuple_desc->attrs[i];
    auto block = GetBlock(attr.attnum - 1, current_block_id_);
    if (block == nullptr) {
      slot->tts_isnull[i] = true;
      continue;
    }
    auto offset = offset_in_block_;
    switch (block->column_->type_) {
      case BlockType::INT: {
        auto val = reinterpret_cast<const int32_t *>(block->data_.c_str() + offset * block->width_);
        slot->tts_values[i] = Int32GetDatum(*val);
        break;
      }
      case BlockType::STR: {
        auto val = reinterpret_cast<const char *>(block->data_.c_str() + offset * block->width_);
        auto oldcontext = MemoryContextSwitchTo(ectx_str_slab_memory_);
        auto size = STR_SIZE + VARHDRSZ;
        auto result = static_cast<text *>(palloc0(size));
        auto len = strlen(val);
        SET_VARSIZE(result, len + VARHDRSZ);
        memcpy(VARDATA(result), val, len);
        slot->tts_values[i] = PointerGetDatum(result);
        MemoryContextSwitchTo(oldcontext);
        break;
      }
      case BlockType::FLOAT: {
        auto val = reinterpret_cast<const float *>(block->data_.c_str() + offset * block->width_);
        slot->tts_values[i] = Float4GetDatum(*val);
        break;
      }
      default:;  // ignoring unsupported type
    }
  }
  ExecStoreVirtualTuple(slot);

  if (scan_tlist_ != nullptr) {
    ExecClearTuple(rel_tuple_slot_);
    for (int i = 0; i < tuple_desc->natts; ++i) {
      auto tle = static_cast<TargetEntry *>(list_nth(scan_tlist_, i));
      auto var = reinterpret_cast<Var *>(tle->expr);
      rel_tuple_slot_->tts_values[var->varattno - 1] = slot->tts_values[i];
    }
    ExecStoreVirtualTuple(rel_tuple_slot_);
  }
  ++offset_in_block_;
}
