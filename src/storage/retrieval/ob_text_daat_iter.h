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

#ifndef OB_TEXT_DAAT_ITER_H_
#define OB_TEXT_DAAT_ITER_H_

#include "query/ob_sql_define.h"
#include "ob_inv_idx_param_estimator.h"
#include "ob_sparse_bmw_iter.h"
#include "ob_sparse_daat_iter.h"
#include "ob_text_retrieval_token_iter.h"

namespace oceanbase
{
namespace sql
{
  class ObDASScanIter;
}
namespace storage
{

struct ObTextDaaTParam
{
  ObTextDaaTParam()
    : dim_iters_(nullptr), base_param_(nullptr), allocator_(nullptr), relevance_collector_(nullptr),
      bm25_param_est_ctx_(), mode_flag_(sql::ObMatchAgainstMode::NATURAL_LANGUAGE_MODE),
      function_lookup_mode_(false), min_should_match_(0) {}
  ~ObTextDaaTParam() {}
  TO_STRING_KV(K_(base_param), KP_(dim_iters), K_(bm25_param_est_ctx),  K_(mode_flag), K_(function_lookup_mode));
  ObIArray<ObISRDaaTDimIter *> *dim_iters_;
  ObSparseRetrievalMergeParam *base_param_;
  common::ObArenaAllocator *allocator_;
  ObSRDaaTRelevanceCollector *relevance_collector_;
  ObBM25ParamEstCtx bm25_param_est_ctx_;
  sql::ObMatchAgainstMode mode_flag_;
  bool function_lookup_mode_;
  int64_t min_should_match_;
};

class ObTextDaaTIter final : public ObSRDaaTIterImpl
{
public:
  ObTextDaaTIter() : ObSRDaaTIterImpl(),
      bm25_param_estimator_(),
      mode_flag_(sql::ObMatchAgainstMode::NATURAL_LANGUAGE_MODE),
      function_lookup_mode_(false),
      min_should_match_(0),
      union_mode_checked_(false),
      union_mode_(false),
      union_cursors_(nullptr),
      union_out_ids_(nullptr),
      union_out_bytes_(nullptr) {}
  virtual ~ObTextDaaTIter() { reset(); }

  int init(const ObTextDaaTParam &param);
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  virtual void reuse(const bool switch_tablet = false) override;
  virtual void reset() override;
protected:
  virtual int pre_process() override;
private:
  // When neither relevance nor a filter on it is needed and document ids are unsigned integers,
  // the result is the plain union of the sorted posting lists, merged on raw ids.
  struct UnionCursor
  {
    const ObTextDocIdKey *keys_;
    int64_t count_;
    int64_t pos_;
    bool end_;
  };
  int check_union_mode();
  int union_next_rows(const int64_t capacity, int64_t &count);
  int load_union_cursor(UnionCursor &cursor, const int64_t dim_idx);
  void reset_union_cursors();
protected:
  ObBM25ParamEstimator bm25_param_estimator_;
  sql::ObMatchAgainstMode mode_flag_;
  bool function_lookup_mode_;
  int64_t min_should_match_;
private:
  bool union_mode_checked_;
  bool union_mode_;
  UnionCursor *union_cursors_;
  ObTextDocIdKey *union_out_ids_;
  char *union_out_bytes_;
  DISALLOW_COPY_AND_ASSIGN(ObTextDaaTIter);
};


class ObTextBMWIter final : public ObSRBMWIterImpl
{
public:
  ObTextBMWIter();
  virtual ~ObTextBMWIter() { reset(); }
  virtual void reuse(const bool switch_tablet = false) override;
  virtual void reset() override;
  int init(const ObTextDaaTParam &param);
protected:
  virtual int get_next_rows(const int64_t capacity, int64_t &count) override;
  virtual int init_before_wand_process() override;
protected:
  ObBM25ParamEstimator bm25_param_estimator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTextBMWIter);
};

} // namespace storage
} // namespace oceanbase

#endif
