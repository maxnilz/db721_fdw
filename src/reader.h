#pragma once

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/utils/fmgrprotos.h"
#include "../../../../src/include/mb/pg_wchar.h"
#include "../../../../src/include/common/jsonapi.h"
#include "../../../../src/include/access/tupdesc.h"
#include "../../../../src/include/executor/tuptable.h"
#include "../../../../src/include/executor/executor.h"
#include "../../../../src/include/utils/builtins.h"
}
// TODO(WAN): Hack.
//  Because PostgreSQL tries to be portable, it makes a bunch of global
//  definitions that can make your C++ libraries very sad.
//  We're just going to undefine those.
#undef vsnprintf
#undef snprintf
#undef vsprintf
#undef sprintf
#undef vfprintf
#undef fprintf
#undef vprintf
#undef printf
#undef gettext
#undef dgettext
#undef ngettext
#undef dngettext
// clang-format on

#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

constexpr auto BASE_SIZE = 4;
constexpr auto STR_SIZE = 32;
constexpr auto INT_FLOAT_SIZE = 4;
constexpr auto INVALID_BLOCK = -1;
constexpr auto INVALID_BLOCK_IDX = -1;

enum BlockType { INVALID, INT, STR, FLOAT };

struct BlockStat {
  BlockType type_;
  int64 num_;
  union {
    struct {
      int64 min_;
      int64 max_;
    } int_;
    struct {
      char *min_;
      char *max_;
      int64 min_len_;
      int64 max_len_;
    } str_;
    struct {
      float min_;
      float max_;
    } float_;
  };
};

struct Column {
  char *name_;
  BlockType type_;
  int64 num_blocks_;
  int64 start_offset_;
  int64 stats_size_;
  int64 stats_length_;
  BlockStat *stats_;
};

struct Metadata {
  char *table_;
  int64 columns_size_;
  int64 column_length_;
  Column *columns_;
  int64 max_values_per_block_;

  // used by parse
  char **parse_stack_;
  int64 stack_size_;
  int64 stack_top_;

  // helper metrics
  int64 total_rows_;
  int64 num_blocks_;
  int64 *block_values_;
  char *filename_;
};

static_assert(std::is_trivial_v<Metadata>);
static_assert(std::is_standard_layout_v<Metadata>);

inline void Require(bool condition, const std::string &message = "Requirement failed") {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class Reader {
 public:
  struct Block {
    Block(std::string data, Column *column, int64 width) : data_(std::move(data)), column_(column), width_(width){};
    std::string data_;
    Column *column_;
    int64 width_;
  };

  Reader(std::string name, Metadata *metadata, TupleDesc tuple_desc, std::vector<int16> candidate_blocks,
         ExprState *qual, MemoryContext ectx_str_slab_memory, TupleTableSlot *rel_tuple_slot, List *scan_tlist);
  ~Reader();

  static auto ParseMetadata(const std::string &filename) -> Metadata *;
  static auto GetColumnByName(const Metadata *md, const std::string &name) -> Column *;

  auto GetBlock(int16 attrunm, int16 block_index) -> Block *;
  void Next(TupleTableSlot *slot, ExprContext *econtext);
  void Next(TupleTableSlot *slot);
  void Reset() {
    eof_reached_ = true;
    if (!candidate_blocks_.empty()) {
      current_candidate_blocks_idx_ = 0;
      current_block_id_ = candidate_blocks_[current_candidate_blocks_idx_];
      eof_reached_ = false;
    }
    offset_in_block_ = 0;
  };

 private:
  std::string name_;
  Metadata *metadata_;
  std::vector<int16> candidate_blocks_;
  ExprState *qual_;
  MemoryContext ectx_str_slab_memory_;
  TupleTableSlot *rel_tuple_slot_;
  List *scan_tlist_;
  int64 offset_in_block_{0};
  bool eof_reached_{true};
  int16 current_block_id_{INVALID_BLOCK};
  int16 current_candidate_blocks_idx_{INVALID_BLOCK_IDX};
  std::ifstream is_;
  // attno -> column
  std::unordered_map<int16, Column *> columns_map_;
  // two dimension array: block_id/attno
  std::vector<std::vector<std::shared_ptr<Block>>> blocks_;
};
