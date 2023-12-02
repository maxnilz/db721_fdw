// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include <functional>
#include "reader.h"

// clang-format off
extern "C" {
#include "nodes/execnodes.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "../../../../src/include/foreign/foreign.h"
#include "../../../../src/include/commands/defrem.h"
#include "../../../../src/include/utils/typcache.h"
#include "../../../../src/include/access/nbtree.h"
#include "../../../../src/include/access/table.h"
#include "../../../../src/include/utils/lsyscache.h"
#include "../../../../src/include/optimizer/pathnode.h"
#include "../../../../src/include/optimizer/planmain.h"
#include "../../../../src/include/optimizer/optimizer.h"
#include "../../../../src/include/optimizer/restrictinfo.h"
#include "../../../../src/include/optimizer/tlist.h"
#include "../../../../src/include/nodes/makefuncs.h"
}
// clang-format on

constexpr auto OP_EQ = "=";
constexpr auto OP_NEQ = "!=";
constexpr auto OP_INEQ = "<>";
constexpr auto OP_LESSEQ = "<=";
constexpr auto OP_LESS = "<";
constexpr auto OP_GREATEREQ = ">=";
constexpr auto OP_GREATER = ">";

struct DB721FdwPlanState {
  Metadata *md_;
  List *matched_blocks_;
  List *db721_conds_;
  List *scan_conds_;
};

static void FindCmpFunc(FmgrInfo *finfo, Oid type1, Oid type2) {
  Oid cmp_proc_oid;
  TypeCacheEntry *tce_1;
  TypeCacheEntry *tce_2;

  tce_1 = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);
  tce_2 = lookup_type_cache(type2, TYPECACHE_BTREE_OPFAMILY);

  cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf, tce_1->btree_opintype, tce_2->btree_opintype, BTORDER_PROC);
  fmgr_info(cmp_proc_oid, finfo);
}

static auto IsSupportedExpr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr) -> bool {
  if (!IsA(expr, OpExpr)) {
    return false;
  }
  auto *op_expr = reinterpret_cast<OpExpr *>(expr);
  if (list_length(op_expr->args) != 2) {
    return false;
  }

  auto op_name = get_opname(op_expr->opno);
  if (strcmp(op_name, OP_EQ) != 0 && strcmp(op_name, OP_NEQ) != 0 && strcmp(op_name, OP_INEQ) != 0 &&
      strcmp(op_name, OP_GREATER) != 0 && strcmp(op_name, OP_GREATEREQ) != 0 && strcmp(op_name, OP_LESS) != 0 &&
      strcmp(op_name, OP_LESSEQ) != 0) {
    return false;
  }

  // check operands
  auto left = reinterpret_cast<Expr *> linitial(op_expr->args);
  auto right = reinterpret_cast<Expr *> lsecond(op_expr->args);

  // escape dummy RelabelType
  if (IsA(left, RelabelType)) {
    auto dummy_node = reinterpret_cast<RelabelType *>(left);
    left = dummy_node->arg;
  }
  if (IsA(right, RelabelType)) {
    auto dummy_node = reinterpret_cast<RelabelType *>(right);
    right = dummy_node->arg;
  }

  // check op expr
  if (IsA(left, Var) && IsA(right, Const)) {
    return true;
  }
  if (IsA(left, Const) && IsA(right, Var)) {
    return true;
  }
  return false;
}

static void ClassifyConditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds, List **conds, List **sconds) {
  ListCell *lc;

  *conds = NIL;
  *sconds = NIL;

  /*
   * the conjunction of the element of input_conds are AND.
   */
  foreach (lc, input_conds) {
    RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
    Expr *clause = ri->clause;
    if (ri->orclause != nullptr) {
      // TODO: support OR clause
    }
    if (IsSupportedExpr(root, baserel, clause)) {
      *conds = lappend(*conds, ri);
    } else {
      *sconds = lappend(*sconds, ri);
    }
  }
}

static auto BlockMinToDatum(BlockStat *bs, Oid consttype) -> Datum {
  switch (bs->type_) {
    case BlockType::INT:
      return Int32GetDatum(bs->int_.min_);
    case BlockType::FLOAT:
      return Float4GetDatum(bs->float_.min_);
    case BlockType::STR:
      return PointerGetDatum(cstring_to_text(bs->str_.min_));
    default:
      return PointerGetDatum(NULL);
  }
}

static auto BlockMaxToDatum(BlockStat *bs, Oid consttype) -> Datum {
  switch (bs->type_) {
    case BlockType::INT:
      return Int32GetDatum(bs->int_.max_);
    case BlockType::FLOAT:
      return Float4GetDatum(bs->float_.max_);
    case BlockType::STR:
      return PointerGetDatum(cstring_to_text(bs->str_.max_));
    default:
      return PointerGetDatum(NULL);
  }
}

static auto EstimateSize(TupleDesc td, const Metadata *md, const List *conds, List **matched_blocks) -> int64 {
  if (conds == nullptr) {
    for (int i = 0; i < md->num_blocks_; ++i) {
      *matched_blocks = lappend_int(*matched_blocks, i);
    }
    return md->total_rows_;
  }
  using BlocksIndex = std::vector<int64>;
  std::vector<BlocksIndex> columns_index;
  columns_index.reserve(conds->length);  // one column per cond

  ListCell *lc;
  foreach (lc, conds) {
    RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
    Expr *clause = ri->clause;
    if (ri->orclause != nullptr) {
      // TODO: support OR clause
    }
    Assert(IsA(clause, OpExpr));

    auto expr = reinterpret_cast<OpExpr *>(clause);
    auto left = reinterpret_cast<Expr *> linitial(expr->args);
    auto right = reinterpret_cast<Expr *> lsecond(expr->args);

    // escape dummy RelabelType
    if (IsA(left, RelabelType)) {
      auto dummy_node = reinterpret_cast<RelabelType *>(left);
      left = dummy_node->arg;
    }
    if (IsA(right, RelabelType)) {
      auto dummy_node = reinterpret_cast<RelabelType *>(right);
      right = dummy_node->arg;
    }

    // var, val and opno
    Var *var;
    Const *val;
    auto opno = expr->opno;
    if (IsA(left, Var) && IsA(right, Const)) {
      var = reinterpret_cast<Var *>(left);
      val = reinterpret_cast<Const *>(right);
    } else if (IsA(left, Const) && IsA(right, Var)) {
      var = reinterpret_cast<Var *>(right);
      val = reinterpret_cast<Const *>(left);
      opno = get_commutator(opno);
    } else {
      elog(ERROR, "found unsupported expr");
    }

    FmgrInfo finfo;
    FindCmpFunc(&finfo, val->consttype, var->vartype);

    std::unordered_map<std::string, std::function<bool(BlockStat &)>> actions;
    actions[OP_EQ] = [&](BlockStat &bs) -> bool {
      auto min = BlockMinToDatum(&bs, val->consttype);
      auto max = BlockMaxToDatum(&bs, val->consttype);
      auto res = FunctionCall2Coll(&finfo, val->constcollid, val->constvalue, min);
      int32 l = DatumGetInt32(res);
      res = FunctionCall2Coll(&finfo, val->constcollid, val->constvalue, max);
      int32 u = DatumGetInt32(res);
      return l >= 0 && u <= 0;
    };
    actions[OP_NEQ] = [&](BlockStat &bs) -> bool {
      auto min = BlockMinToDatum(&bs, val->consttype);
      auto max = BlockMaxToDatum(&bs, val->consttype);
      auto res = FunctionCall2Coll(&finfo, val->constcollid, val->constvalue, min);
      int32 l = DatumGetInt32(res);
      res = FunctionCall2Coll(&finfo, val->constcollid, val->constvalue, max);
      int32 u = DatumGetInt32(res);
      if (l == 0 && u == 0) {
        return false;
      }
      return true;
    };
    actions[OP_INEQ] = [&](BlockStat &bs) -> bool { return actions[OP_NEQ](bs); };
    actions[OP_LESS] = [&](BlockStat &bs) -> bool {
      auto min = BlockMinToDatum(&bs, val->consttype);
      auto res = FunctionCall2Coll(&finfo, val->constcollid, val->constvalue, min);
      int32 cmpres = DatumGetInt32(res);
      return cmpres > 0;
    };
    actions[OP_LESSEQ] = [&](BlockStat &bs) -> bool {
      auto min = BlockMinToDatum(&bs, val->consttype);
      auto res = FunctionCall2Coll(&finfo, val->constcollid, val->constvalue, min);
      int32 cmpres = DatumGetInt32(res);
      return cmpres >= 0;
    };
    actions[OP_GREATER] = [&](BlockStat &bs) -> bool {
      auto max = BlockMaxToDatum(&bs, val->consttype);
      auto res = FunctionCall2Coll(&finfo, val->constcollid, val->constvalue, max);
      int32 cmpres = DatumGetInt32(res);
      return cmpres < 0;
    };
    actions[OP_GREATEREQ] = [&](BlockStat &bs) -> bool {
      auto max = BlockMaxToDatum(&bs, val->consttype);
      auto res = FunctionCall2Coll(&finfo, val->constcollid, val->constvalue, max);
      int32 cmpres = DatumGetInt32(res);
      return cmpres <= 0;
    };

    auto op_name = get_opname(opno);
    auto attname = NameStr(TupleDescAttr(td, var->varattno - 1)->attname);
    auto column = Reader::GetColumnByName(md, attname);

    // inspect blocks
    int64 count = 0;
    BlocksIndex blocks_index;
    blocks_index.reserve(column->num_blocks_);
    for (int i = 0; i < column->num_blocks_; ++i) {
      auto &bs = column->stats_[i];
      auto it = actions.find(op_name);
      if (it == actions.end()) {
        elog(ERROR, "unsupported opno %d, %s", opno, op_name);
      }
      if (!it->second(bs)) {
        blocks_index.push_back(0);
        continue;
      }
      count += bs.num_;
      blocks_index.push_back(bs.num_);
    }
    if (count == 0) {
      return 0;  // no block will fulfill the given cond.
                 // since the multiple conds are ANDed,
                 // return zero here immediately.
    }
    columns_index.push_back(blocks_index);
  }
  // take intersections of blocks via the columns index
  int64 count = 0;
  for (int i = 0; i < md->num_blocks_; ++i) {
    bool b = true;
    for (auto &j : columns_index) {
      b = b && j[i] > 0;
    }
    if (b) {
      *matched_blocks = lappend_int(*matched_blocks, i);
      count += md->columns_[0].stats_[0].num_;  // columns & stats are guaranteed to have at least one element.
    }
  }
  return count;
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
  // parse options
  char *filename = nullptr;
  auto *table = GetForeignTable(foreigntableid);
  ListCell *lc;
  foreach (lc, table->options) {
    auto *def = (DefElem *)lfirst(lc);
    if (strcmp(def->defname, "filename") == 0) {
      filename = defGetString(def);
    }
  }
  if (filename == nullptr) {
    elog(ERROR, "no filename option found");
  }

  Metadata *metadata;
  try {
    // parse metadata
    metadata = Reader::ParseMetadata(filename);
  } catch (std::exception &e) {
    elog(ERROR, "parse metadata failed: %s, %s", filename, e.what());
  } catch (...) {
    elog(ERROR, "parse metadata failed:: unknown exception");
  }

  auto rte = root->simple_rte_array[baserel->relid];
  auto rel = table_open(rte->relid, AccessShareLock);
  auto tuple_desc = RelationGetDescr(rel);

  List *conds = nullptr;
  List *scan_conds = nullptr;
  ClassifyConditions(root, baserel, baserel->baserestrictinfo, &conds, &scan_conds);

  List *matched_blocks = nullptr;
  auto estimate_size = EstimateSize(tuple_desc, metadata, conds, &matched_blocks);

  table_close(rel, AccessShareLock);

  elog(LOG, "classified cond: %d %d, matching %d blocks", conds == nullptr ? 0 : conds->length,
       scan_conds == nullptr ? 0 : scan_conds->length, matched_blocks == nullptr ? 0 : matched_blocks->length);

  auto plan_state = (DB721FdwPlanState *)palloc0(sizeof(DB721FdwPlanState));
  plan_state->md_ = metadata;
  plan_state->db721_conds_ = conds;
  plan_state->scan_conds_ = scan_conds;
  plan_state->matched_blocks_ = matched_blocks;

  baserel->rows = estimate_size;
  baserel->tuples = metadata->total_rows_;
  baserel->fdw_private = plan_state;
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
  auto run_cost = baserel->rows * cpu_tuple_cost;
  auto startup_cost = baserel->baserestrictcost.startup;
  auto total_cost = startup_cost + run_cost;

  // we have only one path.
  auto foreign_path = create_foreignscan_path(root, baserel, nullptr, baserel->rows, startup_cost, total_cost, nullptr,
                                              nullptr, nullptr, nullptr);
  add_path(baserel, reinterpret_cast<Path *>(foreign_path));
}

extern "C" ForeignScan *db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                                             ForeignPath *best_path, List *tlist, List *scan_clauses,
                                             Plan *outer_plan) {
  Index scan_relid = best_path->path.parent->relid;

  /* it should be a base rel... */
  Assert(scan_relid > 0);
  Assert(best_path->path.parent->rtekind == RTE_RELATION);

  auto plan_state = (DB721FdwPlanState *)baserel->fdw_private;

  auto fdw_private = list_make1(plan_state);

  // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants
  auto clauses = extract_actual_clauses(plan_state->scan_conds_, false);

  // Projection pushdown
  List *ttlist = NIL;
  ttlist = add_to_flat_tlist(ttlist, pull_var_clause((Node *)baserel->reltarget->exprs, PVC_RECURSE_PLACEHOLDERS));
  ListCell *lc;
  foreach (lc, baserel->baserestrictinfo) {
    RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
    ttlist = add_to_flat_tlist(ttlist, pull_var_clause((Node *)rinfo->clause, PVC_RECURSE_PLACEHOLDERS));
  }
  List *fdw_scan_tlist = NIL;
  foreach (lc, tlist) {
    auto *tle = (TargetEntry *)lfirst(lc);
    fdw_scan_tlist =
        lappend(fdw_scan_tlist, makeTargetEntry(tle->expr, list_length(fdw_scan_tlist) + 1, nullptr, false));
  }
  foreach (lc, ttlist) {
    auto *tle = (TargetEntry *)lfirst(lc);
    if (tlist_member(tle->expr, fdw_scan_tlist) != nullptr) {
      continue; /* already got it */
    }
    fdw_scan_tlist =
        lappend(fdw_scan_tlist, makeTargetEntry(tle->expr, list_length(fdw_scan_tlist) + 1, nullptr, false));
  }

  /* Build the list of columns to be fetched from the foreign server. */
  return make_foreignscan(tlist, clauses, scan_relid, nullptr, fdw_private, fdw_scan_tlist, nullptr, nullptr);
}

static void DestroyScanState(void *arg) {
  auto *reader = static_cast<Reader *>(arg);
  delete reader;
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  auto estate = node->ss.ps.state;
  MemoryContext cxt = estate->es_query_cxt;
  auto reader_cxt = AllocSetContextCreate(cxt, "db721_fdw tuple data", ALLOCSET_DEFAULT_SIZES);

  auto *fsplan = (ForeignScan *)node->ss.ps.plan;
  auto plan_state = (DB721FdwPlanState *)linitial(fsplan->fdw_private);

  try {
    // Parse candidate blocks
    std::vector<int16> candidate_blocks;
    ListCell *lc;
    foreach (lc, plan_state->matched_blocks_) {
      candidate_blocks.push_back(lfirst_int(lc));
    }

    // Init local qual that need to evaluate during the foreign scan.
    // Reduce RestrictInfo list to bare expressions; ignore pseudoconstants
    auto clauses = extract_actual_clauses(plan_state->db721_conds_, false);
    auto qual = ExecInitQual(clauses, (PlanState *)node);

    // Create reader as the private state of foreign scan .
    auto md = plan_state->md_;
    auto slot = node->ss.ss_ScanTupleSlot;
    auto tuple_desc = slot->tts_tupleDescriptor;
    auto ectx_str_slab_memory =
        SlabContextCreate(cxt, "db721_fdw ectx_str_slab_memory", SLAB_LARGE_BLOCK_SIZE, STR_SIZE + VARHDRSZ);

    // Create relation tuple slot for expr evaluation
    auto current_relation = ExecOpenScanRelation(estate, fsplan->scan.scanrelid, eflags);
    auto rel_tupdesc = CreateTupleDescCopy(RelationGetDescr(current_relation));
    auto rel_tuple_slot = MakeTupleTableSlot(rel_tupdesc, &TTSOpsHeapTuple);

    auto fdw_scan_tlist = fsplan->fdw_scan_tlist;
    foreach (lc, fdw_scan_tlist) {
      TargetEntry *tle = lfirst_node(TargetEntry, lc);
      if (!IsA(tle->expr, Var)) {
        elog(ERROR, "expect Var in scan list");
      }
    }
    auto reader = new Reader(md->filename_, md, tuple_desc, candidate_blocks, qual, ectx_str_slab_memory,
                             rel_tuple_slot, fdw_scan_tlist);

    // Enable automatic execution state destruction by using memory context callback
    auto callback = (MemoryContextCallback *)palloc(sizeof(MemoryContextCallback));
    callback->func = DestroyScanState;
    callback->arg = (void *)reader;
    MemoryContextRegisterResetCallback(reader_cxt, callback);

    node->fdw_state = reader;
  } catch (std::exception &e) {
    elog(ERROR, "begin foreign scan failed: %s", e.what());
  } catch (...) {
    elog(ERROR, "begin foreign scan failed: unknown exception");
  }
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {
  auto *reader = reinterpret_cast<Reader *>(node->fdw_state);

  TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
  ExprContext *econtext = node->ss.ps.ps_ExprContext;
  try {
    reader->Next(slot, econtext);
  } catch (std::exception &e) {
    elog(ERROR, "iterate foreign scan failed: %s", e.what());
  } catch (...) {
    elog(ERROR, "iterate foreign scan failed: unknown exception");
  }

  return slot;
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  auto *reader = reinterpret_cast<Reader *>(node->fdw_state);
  reader->Reset();
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  /*
   * Destruction of execution state is done by memory context callback.
   * See DestroyScanState()
   */
}
