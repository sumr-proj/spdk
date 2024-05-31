/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 */

#ifndef SPDK_RAID_SERVICE_INTERNAL_H
#define SPDK_RAID_SERVICE_INTERNAL_H

#include "spdk/queue.h"
#include "atomic_raid.h"
#include "bdev_raid.h"

#define SERVICE_DEBUG

//->
#define __base_desc_from_raid_bdev(raid_bdev, idx) (raid_bdev->base_bdev_info[idx].desc)
#define fl(rebuild) &(rebuild->rebuild_flag)
#define NOT_NEED_REBUILD -1
//->
#define ATOMIC_IS_AREA_STR_CLEAR(ptr) (*ptr == 0)
#define CREATE_AREA_STR_SNAPSHOT(area_srt_ptr) raid_atomic64_read(area_srt_ptr)
#define ATOMIC_INCREMENT(ptr) raid_atomic64_inc(ptr)
#define ATOMIC_DECREMENT(ptr) raid_atomic64_dec(ptr)
#define ATOMIC_EXCHANGE(dest_ptr, exc, src) (raid_atomic64_cmpxchg(dest_ptr, exc, src))
// ->
#define b_BASE_TYPE uint64_t
#define b_BIT_PROECTION(name) b_BASE_TYPE name[SPDK_CEIL_DIV(MATRIX_REBUILD_SIZE, (sizeof(b_BASE_TYPE) * 8))]
#define b_GET_IDX_BP(x) (x / (sizeof(b_BASE_TYPE) * 8))
#define b_GET_SHFT_BP(x) (x % (sizeof(b_BASE_TYPE) * 8))
//

static inline bool
_CAS(ATOMIC_TYPE *ptr, ATOMIC_SNAPSHOT_TYPE exc, ATOMIC_SNAPSHOT_TYPE src)
{
    if (*ptr == exc)
    {
        *ptr = src;
        return true;
    }
    return false;
}

struct iteration_step
{
    int16_t area_idx;
    int64_t iter_idx;
    struct rebuild_cycle_iteration *iteration;
    struct raid_bdev *raid_bdev;
    spdk_bdev_io_completion_cb cb;
};

struct rebuild_cycle_iteration
{
    /* number of broken areas in current area stripe */
    int16_t br_area_cnt;

    /* index of the area stripe for rebuld */
    int64_t iter_idx;

    /* processed areas counter, it increments after completion rebuild a concrete area */
    ATOMIC_DATA(pr_area_cnt);

    /* snapshot of area stripe from rebuild matrix (non atomic) */
    ATOMIC_SNAPSHOT(snapshot);

    /*
     * metadata for current iteration,
     * describing which areas should still be started for rebuild
     * (equals snapshot at initialization stage)
     * (10..010 |-[start rebuild area with index 1]-> 10..000)
     */
    ATOMIC_SNAPSHOT(iter_progress);
};

struct rebuild_progress
{
    /*
     * bit proection of rebuild matrix,
     * where each bit corresponds one line(area stripe) in rebuild matrix
     * (if the line contains broken areas, corresponding bit equels 1 othewise 0)
     */
    b_BIT_PROECTION(area_proection);

    /* number of areas stripes with broken areas */
    uint64_t area_str_cnt;

    /* number of area stripes with processed areas (tried to rebuild all the broken areas) */
    uint64_t clear_area_str_cnt;

    /*
     * To avoid memory overloading, only one area stripe (in need of rebuild)
     * can be processed at a time.
     * The fild describes the rebuild of this area stripe.
     */
    struct rebuild_cycle_iteration cycle_iteration;

    /*
     * Buffers for raid base_bdevs.
     * Each element - SG-buffer (array of iovec);
     * Size of each SG-buffer is size of one memory area in bytes;
     * One element from SG-buffer describes buffer size equals size of one strip in bytes.
     */
    struct iovec *base_bdevs_sg_buf[BASE_BDEVS_MAX_NUM];
};

int
run_rebuild_poller(void *arg);

void continue_rebuild(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg);

void
extern_continue_rebuild(int64_t iter_idx,
                        int16_t area_idx,
                        struct rebuild_cycle_iteration *iteration,
                        struct raid_bdev *raid_bdev);

struct iteration_step *
alloc_cb_arg(int64_t iter_idx, int16_t area_idx, struct rebuild_cycle_iteration *iteration, struct raid_bdev *raid_bdev);

void init_cb_arg(struct iteration_step *iter_info,
                 int64_t iter_idx,
                 int16_t area_idx,
                 struct rebuild_cycle_iteration *iteration,
                 struct raid_bdev *raid_bdev);

void free_cb_arg(struct iteration_step *cb);

void reset_buffer(struct iovec *vec_array, uint32_t len);

uint64_t
get_area_offset(size_t area_idx, size_t area_size, size_t strip_size);

uint64_t
get_area_size(size_t area_size, size_t strip_size);

#ifdef SERVICE_DEBUG

#define PRINT_iteration_step(it) SPDK_ERRLOG("\
\niter_step: \
\n area_idx=%d \
\n iter_idx=%ld \n", (it)->area_idx, (it)->iter_idx)

#define PRINT_rebuild_cycle_iteration(cit) SPDK_ERRLOG("\
\ncycle_iter: \
\n br_area_cnt=%d \
\n iter_idx=%ld \
\n pr_area_cnt=%lu \
\n snapshot=%lu \
\n iter_progress=%lu \n", (cit)->br_area_cnt, (cit)->iter_idx, (cit)->pr_area_cnt, (cit)->snapshot, (cit)->iter_progress)

#define PRINT_rebuild_progress(pr) SPDK_ERRLOG("\
\nprogress: \
\n area_str_cnt=%lu \
\n clear_area_str_cnt=%lu \n", (pr)->area_str_cnt, (pr)->clear_area_str_cnt)

#endif

#endif /* SPDK_RAID_SERVICE_INTERNAL_H */
