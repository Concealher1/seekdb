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

#ifndef OCEANBASE_OBSERVER_OB_AI_COLUMN_WORKER_H_
#define OCEANBASE_OBSERVER_OB_AI_COLUMN_WORKER_H_

#include "lib/ob_define.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace observer
{

// Background thread that completes asynchronous AI column values.
//
// The DML path only enqueues work: one row of __all_ai_column_task per changed
// row and AI column, written with the transaction of the data change.  This
// worker claims those tasks, evaluates the prompt of the AI column against the
// current row, calls the model with the typed output contract and publishes the
// whole request group (all AI columns of one row change share a request_id) in
// a single transaction.
class ObAiColumnWorker : public share::ObThreadPool
{
public:
  ObAiColumnWorker() {}
  virtual ~ObAiColumnWorker() {}
  virtual void run1() override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnWorker);
};

} // namespace observer
} // namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_AI_COLUMN_WORKER_H_ */
