/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_text_daat_iter.h"

namespace oceanbase
{
namespace storage
{
int ObTextDaaTIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRDaaTIterImpl::init(*param.base_param_, *param.dim_iters_,
                                            *param.allocator_, *param.relevance_collector_))) {
  } else if (param.base_param_->need_project_relevance()
             && OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  } else {
    mode_flag_ = param.mode_flag_;
    function_lookup_mode_ = param.function_lookup_mode_;
    min_should_match_ = param.min_should_match_;
    union_mode_checked_ = false;
    union_mode_ = false;
    union_cursors_ = nullptr;
    union_out_ids_ = nullptr;
    union_out_bytes_ = nullptr;
  }
  return ret;
}

void ObTextDaaTIter::reset()
{
  union_mode_checked_ = false;
  union_mode_ = false;
  union_cursors_ = nullptr;
  union_out_ids_ = nullptr;
  union_out_bytes_ = nullptr;
  bm25_param_estimator_.reset();
  // this class can know the type of dim_iters_, but ObSRDaaTIterImpl maybe not
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRDaaTIterImpl::reset();
}

void ObTextDaaTIter::reuse(const bool switch_tablet)
{
  if ((OB_NOT_NULL(dim_iters_) && 0 == dim_iters_->count())) {
    // do nothing
  } else {
    bm25_param_estimator_.reuse(switch_tablet);
  }
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(i))->reuse();
    }
  }
  reset_union_cursors();
  ObSRDaaTIterImpl::reuse(switch_tablet);
}

int ObTextDaaTIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!union_mode_checked_ && OB_FAIL(check_union_mode())) {
  } else if (!union_mode_) {
    ret = ObSRDaaTIterImpl::get_next_rows(capacity, count);
  } else if (OB_UNLIKELY(0 == capacity)) {
  } else if (iter_param_->limit_param_->is_valid() && output_row_cnt_ >= iter_param_->limit_param_->limit_) {
    ret = OB_ITER_END;
  } else {
    ret = union_next_rows(capacity, count);
  }
  return ret;
}

int ObTextDaaTIter::check_union_mode()
{
  int ret = OB_SUCCESS;
  const int64_t dim_cnt = dim_iters_->count();
  const int64_t max_batch_size = OB_MAX(iter_param_->eval_ctx_->max_batch_size_, iter_param_->max_batch_size_);
  union_mode_ = dim_cnt > 0
      && sql::ObMatchAgainstMode::NATURAL_LANGUAGE_MODE == mode_flag_
      // a minimum_should_match above one needs the per-document number of matching terms
      && min_should_match_ <= 1
      && !function_lookup_mode_
      && !iter_param_->need_project_relevance()
      && !iter_param_->need_filter()
      && !iter_param_->need_pushdown_topk()
      && iter_param_->eval_ctx_->is_vectorized()
      && ObTextDocIdKey::is_supported(iter_param_->id_proj_expr_->datum_meta_);
  if (!union_mode_) {
  } else if (OB_ISNULL(union_cursors_ = static_cast<UnionCursor *>(
                 iter_allocator_->alloc(sizeof(UnionCursor) * dim_cnt)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(union_out_ids_ = static_cast<ObTextDocIdKey *>(
                 iter_allocator_->alloc(sizeof(ObTextDocIdKey) * max_batch_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (common::ObUInt64Type != iter_param_->id_proj_expr_->datum_meta_.type_
             && OB_ISNULL(union_out_bytes_ = static_cast<char *>(
                 iter_allocator_->alloc(ObTextDocIdKey::BINARY_ID_LEN * max_batch_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    reset_union_cursors();
  }
  if (OB_SUCC(ret)) {
    union_mode_checked_ = true;
  } else {
    union_mode_ = false;
  }
  return ret;
}

void ObTextDaaTIter::reset_union_cursors()
{
  if (OB_NOT_NULL(union_cursors_) && OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      union_cursors_[i].keys_ = nullptr;
      union_cursors_[i].count_ = 0;
      union_cursors_[i].pos_ = 0;
      union_cursors_[i].end_ = false;
    }
  }
}

int ObTextDaaTIter::load_union_cursor(UnionCursor &cursor, const int64_t dim_idx)
{
  int ret = OB_SUCCESS;
  cursor.pos_ = 0;
  cursor.count_ = 0;
  while (OB_SUCC(ret) && 0 == cursor.count_) {
    if (OB_FAIL(static_cast<ObTextRetrievalDaaTTokenIter *>(dim_iters_->at(dim_idx))
                ->get_next_id_key_batch(cursor.keys_, cursor.count_))) {
      if (OB_ITER_END == ret) {
        cursor.end_ = true;
        cursor.count_ = 0;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int ObTextDaaTIter::union_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  const int64_t dim_cnt = dim_iters_->count();
  const int64_t real_capacity = MIN(capacity, iter_param_->max_batch_size_);
  const common::ObLimitParam *limit_param = iter_param_->limit_param_;
  const bool has_limit = limit_param->is_valid();
  count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt; ++i) {
    UnionCursor &cursor = union_cursors_[i];
    if (!cursor.end_ && cursor.pos_ >= cursor.count_ && OB_FAIL(load_union_cursor(cursor, i))) {
    }
  }
  while (OB_SUCC(ret) && count < real_capacity) {
    ObTextDocIdKey min_id = {0, 0};
    bool found = false;
    for (int64_t i = 0; i < dim_cnt; ++i) {
      const UnionCursor &cursor = union_cursors_[i];
      if (!cursor.end_) {
        const ObTextDocIdKey &id = cursor.keys_[cursor.pos_];
        if (!found || id < min_id) {
          min_id = id;
          found = true;
        }
      }
    }
    if (!found) {
      ret = OB_ITER_END;
    } else {
      int64_t matched_cnt = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < dim_cnt; ++i) {
        UnionCursor &cursor = union_cursors_[i];
        if (!cursor.end_ && cursor.keys_[cursor.pos_] == min_id) {
          ++matched_cnt;
          if (++cursor.pos_ >= cursor.count_ && OB_FAIL(load_union_cursor(cursor, i))) {
          }
        }
      }
      if (OB_FAIL(ret) || matched_cnt < min_should_match_) {
      } else if (FALSE_IT(++input_row_cnt_)) {
      } else if (has_limit && input_row_cnt_ <= limit_param->offset_) {
      } else {
        union_out_ids_[count++] = min_id;
        ++output_row_cnt_;
        if (has_limit && output_row_cnt_ >= limit_param->limit_) {
          ret = OB_ITER_END;
        }
      }
    }
  }
  if ((OB_SUCC(ret) || OB_ITER_END == ret) && count > 0) {
    ObExpr *id_proj_expr = iter_param_->id_proj_expr_;
    ObEvalCtx *eval_ctx = iter_param_->eval_ctx_;
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx);
    sql::ObBitVector &id_evaluated_flags = id_proj_expr->get_evaluated_flags(*eval_ctx);
    for (int64_t i = 0; i < count; ++i) {
      guard.set_batch_idx(i);
      ObDatum &id_datum = id_proj_expr->locate_datum_for_write(*eval_ctx);
      if (OB_ISNULL(union_out_bytes_)) {
        id_datum.set_uint(union_out_ids_[i].lo_);
      } else {
        // restore the binary id, the buffer is kept until the next batch
        uint64_t hi = union_out_ids_[i].hi_;
        uint64_t lo = union_out_ids_[i].lo_;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        hi = __builtin_bswap64(hi);
        lo = __builtin_bswap64(lo);
#endif
        char *buf = union_out_bytes_ + i * ObTextDocIdKey::BINARY_ID_LEN;
        MEMCPY(buf, &hi, sizeof(uint64_t));
        MEMCPY(buf + sizeof(uint64_t), &lo, sizeof(uint64_t));
        id_datum.set_string(buf, ObTextDocIdKey::BINARY_ID_LEN);
      }
      id_evaluated_flags.set(i);
    }
    id_proj_expr->set_evaluated_projected(*eval_ctx);
  }
  return ret;
}

int ObTextDaaTIter::pre_process()
{
  int ret = OB_SUCCESS;
  if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (iter_param_->need_project_relevance()) {
    if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
    }
  }
  return ret;
}

ObTextBMWIter::ObTextBMWIter()
  : ObSRBMWIterImpl(),
    bm25_param_estimator_() {}

void ObTextBMWIter::reuse(const bool switch_tablet)
{
  bm25_param_estimator_.reuse(switch_tablet);
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reuse();
    }
  }
  ObSRBMWIterImpl::reuse(switch_tablet);
}

void ObTextBMWIter::reset()
{
  bm25_param_estimator_.reset();
  if (OB_NOT_NULL(dim_iters_)) {
    for (int64_t i = 0; i < dim_iters_->count(); ++i) {
      static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i))->reset();
    }
  }
  ObSRBMWIterImpl::reset();
}

int ObTextBMWIter::init(const ObTextDaaTParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.base_param_) || OB_ISNULL(param.dim_iters_) || OB_ISNULL(param.allocator_)
      || OB_ISNULL(param.relevance_collector_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObSRBMWIterImpl::init(*param.base_param_, *param.dim_iters_,
                                           *param.allocator_, *param.relevance_collector_))) {
  } else if (OB_FAIL(bm25_param_estimator_.init(param.bm25_param_est_ctx_))) {
  }
  return ret;
}

int ObTextBMWIter::get_next_rows(const int64_t capacity, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (dim_iters_->count() == 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(bm25_param_estimator_.do_estimation(*iter_param_->eval_ctx_))) {
  }

  if (FAILEDx(ObSRBMWIterImpl::get_next_rows(capacity, count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
    }
  }
  return ret;
}

int ObTextBMWIter::init_before_wand_process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!bm25_param_estimator_.is_estimated())) {
    ret = OB_ERR_UNEXPECTED;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dim_iters_->count(); ++i) {
    ObTextRetrievalBlockMaxIter *block_max_iter = static_cast<ObTextRetrievalBlockMaxIter *>(dim_iters_->at(i));
    if (OB_FAIL(block_max_iter->init_block_max_iter(
        bm25_param_estimator_.get_total_doc_cnt(), bm25_param_estimator_.get_avg_doc_token_cnt()))) {
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
