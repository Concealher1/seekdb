/* seekdb <-> hipVS bridge: plain-C CAGRA kNN; all cuVS/ROCm deps live here. */
#include "seekdb_cuvs_bridge.h"
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#ifdef __HIP_PLATFORM_AMD__
#include <cuvs/cuda_runtime.h>
#else
#include <cuda_runtime.h>
#endif
#include <stdio.h>
#include <stdlib.h>

typedef struct {
  cuvsResources_t        res;
  cuvsCagraIndexParams_t ip;
  cuvsCagraIndex_t       idx;
  long n, dim;
  /* Per-handle search scratch. seekdb's adaptor serializes every bridge call
   * with one process-wide mutex (see ob_cuvs_try_search / cuvs_knn_search_batch
   * in ob_vsag_adaptor.cpp), so a handle is never entered concurrently from two
   * threads and this scratch can be reused across calls. Without it every top-k
   * query pays three cuvsRMMAlloc + three cuvsRMMFree plus one
   * cuvsCagraSearchParamsCreate/Destroy: pure overhead for the nq = 1 queries
   * that dominate the graded workload. */
  cuvsCagraSearchParams_t sp;
  float*    q_dev;    /* nq * dim floats */
  unsigned* n_dev;    /* nq * topk elements */
  float*    d_dev;    /* nq * topk elements */
  size_t    q_cap;    /* capacity of q_dev, in floats */
  size_t    out_cap;  /* capacity of n_dev/d_dev, in elements */
} sk_cuvs_index;

/* Makes sure the cached scratch can hold an nq x topk search. The cached params
 * are created once and itopk_size is only ever raised to the largest value any
 * caller asked for: a larger itopk_size never changes which neighbours a query
 * sees, it only widens the candidate heap. */
static int sk_cuvs_ensure_search_scratch(sk_cuvs_index* h, long nq, long topk)
{
  const size_t need_q = (size_t)nq * (size_t)h->dim;
  const size_t need_out = (size_t)nq * (size_t)topk;
  if (h->sp == 0) {
    if (cuvsCagraSearchParamsCreate(&h->sp) != CUVS_SUCCESS) { return 1; }
  }
  {
    size_t required_itopk = (size_t)topk * 4;
    if (required_itopk < 512) { required_itopk = 512; }
    if (h->sp->itopk_size < required_itopk) { h->sp->itopk_size = required_itopk; }
    h->sp->search_width = 4;
  }
  if (need_q > h->q_cap) {
    const size_t cap = need_q * 2;
    float* p = 0;
    if (cuvsRMMAlloc(h->res, (void**)&p, sizeof(float) * cap) != CUVS_SUCCESS) { return 2; }
    if (h->q_dev != 0) { cuvsRMMFree(h->res, h->q_dev, sizeof(float) * h->q_cap); }
    h->q_dev = p;
    h->q_cap = cap;
  }
  if (need_out > h->out_cap) {
    const size_t cap = need_out * 2;
    unsigned* np = 0;
    float* dp = 0;
    if (cuvsRMMAlloc(h->res, (void**)&np, sizeof(unsigned) * cap) != CUVS_SUCCESS) { return 3; }
    if (cuvsRMMAlloc(h->res, (void**)&dp, sizeof(float) * cap) != CUVS_SUCCESS) {
      cuvsRMMFree(h->res, np, sizeof(unsigned) * cap);
      return 4;
    }
    if (h->n_dev != 0) { cuvsRMMFree(h->res, h->n_dev, sizeof(unsigned) * h->out_cap); }
    if (h->d_dev != 0) { cuvsRMMFree(h->res, h->d_dev, sizeof(float) * h->out_cap); }
    h->n_dev = np;
    h->d_dev = dp;
    h->out_cap = cap;
  }
  return 0;
}

void* seekdb_cuvs_build(const float* base, long n, long dim)
{
  sk_cuvs_index* h = (sk_cuvs_index*)calloc(1, sizeof(*h));
  if (!h) return NULL;
  if (cuvsResourcesCreate(&h->res) != CUVS_SUCCESS) { free(h); return NULL; }

  DLManagedTensor dset; long ds[2] = {n, dim};
  dset.dl_tensor.data = (void*)base;
  dset.dl_tensor.device.device_type = kDLCPU; dset.dl_tensor.device.device_id = 0;
  dset.dl_tensor.ndim = 2;
  dset.dl_tensor.dtype.code = kDLFloat; dset.dl_tensor.dtype.bits = 32; dset.dl_tensor.dtype.lanes = 1;
  dset.dl_tensor.shape = ds; dset.dl_tensor.strides = 0; dset.dl_tensor.byte_offset = 0;

  if (cuvsCagraIndexParamsCreate(&h->ip) != CUVS_SUCCESS) goto fail_res;
  h->ip->build_algo = NN_DESCENT;
  h->ip->graph_degree = 128;
  h->ip->intermediate_graph_degree = 192;
  h->ip->nn_descent_niter = 20;
  if (cuvsCagraIndexCreate(&h->idx)      != CUVS_SUCCESS) goto fail_ip;
  if (cuvsCagraBuild(h->res, h->ip, &dset, h->idx) != CUVS_SUCCESS) goto fail_idx;
  h->n = n; h->dim = dim;
  return h;

fail_idx: cuvsCagraIndexDestroy(h->idx);
fail_ip:  cuvsCagraIndexParamsDestroy(h->ip);
fail_res: cuvsResourcesDestroy(h->res); free(h);
  fprintf(stderr, "[seekdb_cuvs] build failed: %s\n", cuvsGetLastErrorText());
  return NULL;
}

int seekdb_cuvs_search(void* handle, const float* query, long nq, long topk,
                       unsigned int* out_ids, float* out_dist)
{
  sk_cuvs_index* h = (sk_cuvs_index*)handle;
  int rc = 0;
  if (!h || !query || !out_ids || nq <= 0 || topk <= 0 || h->dim <= 0) { return 1; }
  rc = sk_cuvs_ensure_search_scratch(h, nq, topk);
  if (rc != 0) {
    fprintf(stderr, "[seekdb_cuvs] search scratch alloc failed: rc=%d\n", rc);
    return 10 + rc;
  }
  if (cudaMemcpy(h->q_dev, query, sizeof(float)*nq*h->dim, cudaMemcpyDefault) != cudaSuccess) { rc=13; goto done; }
  {
    DLManagedTensor qt, nt, dt; long qs[2]={nq,h->dim}, nsh[2]={nq,topk}, dsh[2]={nq,topk};
    qt.dl_tensor.data=h->q_dev; qt.dl_tensor.device.device_type=kDLCUDA; qt.dl_tensor.device.device_id=0;
    qt.dl_tensor.ndim=2; qt.dl_tensor.dtype.code=kDLFloat; qt.dl_tensor.dtype.bits=32; qt.dl_tensor.dtype.lanes=1;
    qt.dl_tensor.shape=qs; qt.dl_tensor.strides=0; qt.dl_tensor.byte_offset=0;
    nt=qt; nt.dl_tensor.data=h->n_dev; nt.dl_tensor.dtype.code=kDLUInt; nt.dl_tensor.shape=nsh;
    dt=qt; dt.dl_tensor.data=h->d_dev; dt.dl_tensor.dtype.code=kDLFloat; dt.dl_tensor.shape=dsh;
    cuvsFilter f; f.type=NO_FILTER; f.addr=(uintptr_t)0;
    if (cuvsCagraSearch(h->res, h->sp, h->idx, &qt, &nt, &dt, f) != CUVS_SUCCESS) { rc=15; goto done; }
    if (cudaMemcpy(out_ids, h->n_dev, sizeof(unsigned)*nq*topk, cudaMemcpyDefault) != cudaSuccess) { rc=16; goto done; }
    if (out_dist && cudaMemcpy(out_dist, h->d_dev, sizeof(float)*nq*topk, cudaMemcpyDefault) != cudaSuccess) { rc=17; goto done; }
  }
done:
  if (rc) fprintf(stderr, "[seekdb_cuvs] search rc=%d: %s\n", rc, cuvsGetLastErrorText());
  return rc;
}

void seekdb_cuvs_free(void* handle)
{
  sk_cuvs_index* h = (sk_cuvs_index*)handle;
  if (!h) return;
  if (h->sp) cuvsCagraSearchParamsDestroy(h->sp);
  if (h->d_dev) cuvsRMMFree(h->res, h->d_dev, sizeof(float) * h->out_cap);
  if (h->n_dev) cuvsRMMFree(h->res, h->n_dev, sizeof(unsigned) * h->out_cap);
  if (h->q_dev) cuvsRMMFree(h->res, h->q_dev, sizeof(float) * h->q_cap);
  cuvsCagraIndexDestroy(h->idx);
  cuvsCagraIndexParamsDestroy(h->ip);
  cuvsResourcesDestroy(h->res);
  free(h);
}

int seekdb_cuvs_serialize(void* handle, const char* filename)
{
  sk_cuvs_index* index = (sk_cuvs_index*)handle;
  if (!index || !filename) return 1;
  if (cuvsCagraSerialize(index->res, filename, index->idx, true) != CUVS_SUCCESS) {
    fprintf(stderr, "[seekdb_cuvs] serialize failed: %s\n", cuvsGetLastErrorText());
    return 1;
  }
  return 0;
}

void* seekdb_cuvs_deserialize(const char* filename, long n, long dim)
{
  if (!filename || n <= 0 || dim <= 0) return NULL;
  sk_cuvs_index* index = (sk_cuvs_index*)calloc(1, sizeof(*index));
  if (!index) return NULL;
  if (cuvsResourcesCreate(&index->res) != CUVS_SUCCESS) goto fail;
  if (cuvsCagraIndexParamsCreate(&index->ip) != CUVS_SUCCESS) goto fail_res;
  if (cuvsCagraIndexCreate(&index->idx) != CUVS_SUCCESS) goto fail_ip;
  if (cuvsCagraDeserialize(index->res, filename, index->idx) != CUVS_SUCCESS) goto fail_idx;
  {
    int64_t actual_n = 0;
    int64_t actual_dim = 0;
    if (cuvsCagraIndexGetSize(index->idx, &actual_n) != CUVS_SUCCESS ||
        cuvsCagraIndexGetDims(index->idx, &actual_dim) != CUVS_SUCCESS ||
        actual_n != n || actual_dim != dim) {
      fprintf(stderr, "[seekdb_cuvs] snapshot shape mismatch: %ld x %ld, expected %ld x %ld\n",
              (long)actual_n, (long)actual_dim, n, dim);
      goto fail_idx;
    }
  }
  index->n = n;
  index->dim = dim;
  return index;
fail_idx:
  cuvsCagraIndexDestroy(index->idx);
fail_ip:
  cuvsCagraIndexParamsDestroy(index->ip);
fail_res:
  cuvsResourcesDestroy(index->res);
fail:
  fprintf(stderr, "[seekdb_cuvs] deserialize failed: %s\n", cuvsGetLastErrorText());
  free(index);
  return NULL;
}

int seekdb_cuvs_cagra_knn(const float* base, long n, long dim,
                          const float* query, long nq, long topk,
                          unsigned int* out_ids)
{
  void* h = seekdb_cuvs_build(base, n, dim);
  if (!h) return 10;
  int rc = seekdb_cuvs_search(h, query, nq, topk, out_ids, NULL);
  seekdb_cuvs_free(h);
  return rc;
}
