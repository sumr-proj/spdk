/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2022 Intel Corporation.
 *   All rights reserved.
 */
#include "bdev_raid.h"
#include "service.h"

#include "spdk/likely.h"
#include "spdk/log.h"
#include "spdk/util.h"

struct raid1_info {
	/* The parent raid bdev */
	struct raid_bdev *raid_bdev;
};

#ifdef SERVICE_DEBUG

#define PRINT_rebuild_first_stage_cb(fs) SPDK_ERRLOG("\
\nfirst_stage_cb: \
\n buf_idx=%u \
\n pd_lba=%lu \
\n pd_blocks=%lu \n", (fs)->buf_idx, (fs)->pd_lba, (fs)->pd_blocks)

#endif

struct rebuild_first_stage_cb
{
	uint8_t buf_idx;
	uint64_t pd_lba;
	uint64_t pd_blocks;
	struct rebuild_progress *cycle_progress;
	struct raid_bdev *raid_bdev;
	spdk_bdev_io_completion_cb cb;
};


/* Find the bdev index of the current IO request */
static uint32_t
get_current_bdev_idx(struct spdk_bdev_io *bdev_io, struct raid_bdev_io *raid_io, uint32_t *bdev_idx)
{
	for (uint8_t i = 0; i < raid_io->raid_bdev->num_base_bdevs; i++) {
		if (raid_io->raid_bdev->base_bdev_info[i].name == bdev_io->bdev->name) {
			*bdev_idx = i;
			return 0;
		}
	}
	return -ENODEV;
}

/* Allows to define the memory_rebuild_areas that are involved in current IO request */
static void
get_io_area_range(struct spdk_bdev_io *bdev_io, struct raid_bdev *raid_bdev, uint64_t *offset,
		  uint64_t *num)
{
	/* blocks */
	uint64_t offset_blocks = bdev_io->u.bdev.offset_blocks;
	uint64_t num_blocks = bdev_io->u.bdev.num_blocks;

	/* blocks -> strips */
	uint64_t offset_strips = (offset_blocks) / raid_bdev->strip_size;
	uint64_t num_strips = SPDK_CEIL_DIV(offset_blocks + num_blocks,
					    raid_bdev->strip_size) - offset_strips;

	/* strips -> areas */
	uint64_t strips_per_area = raid_bdev->rebuild->strips_per_area;

	uint64_t offset_areas = offset_strips / strips_per_area;
	uint64_t num_areas = SPDK_CEIL_DIV(offset_strips + num_strips, strips_per_area) - offset_areas;

	*offset = offset_areas;
	*num = num_areas;
}

/* Write a broken block to the rebuild_matrix */
static void
write_in_rbm_broken_block(struct spdk_bdev_io *bdev_io, struct raid_bdev_io *raid_io,
			  uint32_t bdev_idx)
{
	uint64_t offset_areas = 0;
	uint64_t num_areas = 0;

	get_io_area_range(bdev_io, raid_io->raid_bdev, &offset_areas, &num_areas);
	for (uint64_t i = offset_areas; i < offset_areas + num_areas; i++) {
		uint64_t *area = &raid_io->raid_bdev->rebuild->rebuild_matrix[i];
		SPDK_SET_BIT(area, bdev_idx);
	}
}

/* Determine if a device needs a rebuild or not */
static void
get_bdev_rebuild_status(struct raid_bdev *raid_bdev, struct spdk_bdev_io *bdev_io, uint8_t bdev_idx)
{
	uint64_t offset_areas = 0;
	uint64_t num_areas = 0;

	get_io_area_range(bdev_io, raid_bdev, &offset_areas, &num_areas);
	for (uint64_t i = offset_areas; i < offset_areas + num_areas; i++) {
		uint64_t area = raid_bdev->rebuild->rebuild_matrix[i];
		if (SPDK_TEST_BIT(&area, bdev_idx)) {
			SPDK_SET_BIT(&(raid_bdev->rebuild->rebuild_flag), REBUILD_FLAG_NEED_REBUILD);
		}
	}
}

static void
raid1_bdev_io_completion(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid_bdev_io *raid_io = cb_arg;
	uint32_t bdev_idx = 0;

	get_current_bdev_idx(bdev_io, raid_io, &bdev_idx);


	if (!success) {
		write_in_rbm_broken_block(bdev_io, raid_io, bdev_idx);
	}

	spdk_bdev_free_io(bdev_io);

	raid_bdev_io_complete_part(raid_io, 1, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid1_submit_rw_request(struct raid_bdev_io *raid_io);

static void
_raid1_submit_rw_request(void *_raid_io)
{
	struct raid_bdev_io *raid_io = _raid_io;

	raid1_submit_rw_request(raid_io);
}

static void
raid1_init_ext_io_opts(struct spdk_bdev_io *bdev_io, struct spdk_bdev_ext_io_opts *opts)
{
	memset(opts, 0, sizeof(*opts));
	opts->size = sizeof(*opts);
	opts->memory_domain = bdev_io->u.bdev.memory_domain;
	opts->memory_domain_ctx = bdev_io->u.bdev.memory_domain_ctx;
	opts->metadata = bdev_io->u.bdev.md_buf;
}

static int
raid1_submit_read_request(struct raid_bdev_io *raid_io)
{
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct spdk_bdev_ext_io_opts io_opts;
	uint8_t idx = 0;
	struct raid_base_bdev_info *base_info;
	struct spdk_io_channel *base_ch = NULL;
	uint64_t pd_lba, pd_blocks;
	int ret;

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		base_ch = raid_io->raid_ch->base_channel[idx];
		if (base_ch != NULL) {
			get_bdev_rebuild_status(raid_bdev, bdev_io, idx);
			if (raid_bdev->rebuild->rebuild_flag != REBUILD_FLAG_INIT_CONFIGURATION) {
				break;
			}
			base_ch = NULL;
		}

// TODO REMOVE:
		if (base_info->desc != NULL){
			base_ch = spdk_bdev_get_io_channel(base_info->desc);
			raid_io->raid_ch->base_channel[idx] = base_ch;
			break;
		}
// ------------

		idx++;
	}

	if (base_ch == NULL) {
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
		return 0;
	}

	pd_lba = bdev_io->u.bdev.offset_blocks;
	pd_blocks = bdev_io->u.bdev.num_blocks;

	raid_io->base_bdev_io_remaining = 1;

	raid1_init_ext_io_opts(bdev_io, &io_opts);
	ret = spdk_bdev_readv_blocks_ext(base_info->desc, base_ch,
					 bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
					 pd_lba, pd_blocks, raid1_bdev_io_completion,
					 raid_io, &io_opts);

	if (spdk_likely(ret == 0)) {
		raid_io->base_bdev_io_submitted++;
	} else if (spdk_unlikely(ret == -ENOMEM)) {
		raid_bdev_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
					base_ch, _raid1_submit_rw_request);
		return 0;
	}

	return ret;
}

static int
raid1_submit_write_request(struct raid_bdev_io *raid_io)
{
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct spdk_bdev_ext_io_opts io_opts;
	struct raid_base_bdev_info *base_info;
	struct spdk_io_channel *base_ch;
	uint64_t pd_lba, pd_blocks;
	uint8_t idx;
	uint64_t base_bdev_io_not_submitted;
	int ret = 0;

	pd_lba = bdev_io->u.bdev.offset_blocks;
	pd_blocks = bdev_io->u.bdev.num_blocks;

	if (raid_io->base_bdev_io_submitted == 0) {
		raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs;
	}

	raid1_init_ext_io_opts(bdev_io, &io_opts);
	for (idx = raid_io->base_bdev_io_submitted; idx < raid_bdev->num_base_bdevs; idx++) {
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_io->raid_ch->base_channel[idx];

		if (base_ch == NULL) {
			raid_io->base_bdev_io_submitted++;

			write_in_rbm_broken_block(bdev_io, raid_io, idx);

			raid_bdev_io_complete_part(raid_io, 1, SPDK_BDEV_IO_STATUS_SUCCESS);
			continue;
		}

		ret = spdk_bdev_writev_blocks_ext(base_info->desc, base_ch,
						  bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						  pd_lba, pd_blocks, raid1_bdev_io_completion,
						  raid_io, &io_opts);
		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid_bdev_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid1_submit_rw_request);
				return 0;
			}

			base_bdev_io_not_submitted = raid_bdev->num_base_bdevs -
						     raid_io->base_bdev_io_submitted;
			raid_bdev_io_complete_part(raid_io, base_bdev_io_not_submitted,
						   SPDK_BDEV_IO_STATUS_FAILED);
			return 0;
		}

		raid_io->base_bdev_io_submitted++;
	}

	if (raid_io->base_bdev_io_submitted == 0) {
		ret = -ENODEV;
	}

	return ret;
}

static void
raid1_submit_rw_request(struct raid_bdev_io *raid_io)
{
	struct spdk_bdev_io *bdev_io = spdk_bdev_io_from_ctx(raid_io);
	int ret;

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		ret = raid1_submit_read_request(raid_io);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		ret = raid1_submit_write_request(raid_io);
		break;
	default:
		ret = -EINVAL;
		break;
	}

	if (spdk_unlikely(ret != 0)) {
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
	}
}

static void
init_rebuild(struct raid_bdev *raid_bdev)
{
	uint64_t stripcnt = SPDK_CEIL_DIV(raid_bdev->bdev.blockcnt, raid_bdev->strip_size);
	raid_bdev->rebuild->strips_per_area = SPDK_CEIL_DIV(stripcnt, MATRIX_REBUILD_SIZE);
	raid_bdev->rebuild->num_memory_areas = stripcnt / raid_bdev->rebuild->strips_per_area;
	raid_bdev->rebuild->rebuild_flag = REBUILD_FLAG_INIT_CONFIGURATION;
	SPDK_SET_BIT(&(raid_bdev->rebuild->rebuild_flag), REBUILD_FLAG_INITIALIZED);
}

static void
destruct_rebuild(struct raid_bdev *raid_bdev)
{
	struct raid_rebuild *r1rebuild = raid_bdev->rebuild;

	if (r1rebuild != NULL) {
		free(r1rebuild);
		raid_bdev->rebuild = NULL;
	}
}

static int
raid1_start(struct raid_bdev *raid_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct raid_base_bdev_info *base_info;
	struct raid1_info *r1info;

	r1info = calloc(1, sizeof(*r1info));
	if (!r1info) {
		SPDK_ERRLOG("Failed to allocate RAID1 info device structure\n");
		return -ENOMEM;
	}
	r1info->raid_bdev = raid_bdev;

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		min_blockcnt = spdk_min(min_blockcnt, spdk_bdev_desc_get_bdev(base_info->desc)->blockcnt);
	}

	raid_bdev->bdev.blockcnt = min_blockcnt;
	raid_bdev->module_private = r1info;

	init_rebuild(raid_bdev);

	return 0;
}

static bool
raid1_stop(struct raid_bdev *raid_bdev)
{
	struct raid1_info *r1info = raid_bdev->module_private;

	free(r1info);

	destruct_rebuild(raid_bdev);

	return true;
}

static
void raid1_submit_rebuild_second_stage(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct rebuild_first_stage_cb *info = cb_arg;
	struct raid_bdev *raid_bdev = info->raid_bdev;
	struct rebuild_cycle_iteration *cycle_iteration = &(raid_bdev->rebuild->cycle_progress->cycle_iteration);
	struct raid_base_bdev_info *base_info;
	struct spdk_bdev_desc *desc; /* __base_desc_from_raid_bdev(raid_bdev, idx); */
    struct spdk_io_channel *ch; /* spdk_bdev_get_io_channel(desc); */
	struct iteration_step *cb_arg_new = NULL;
	uint8_t idx = 0;
	int ret = 0;

	if (!success)
	{
		//TODO: Handle this case (mb add new flag FIRST_STAGE_ERROR)
		SPDK_WARNLOG("Problem before firs rebuild stage RAID1\n");
		return;
	}

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		desc = base_info->desc;

		if (!SPDK_TEST_BIT(&(cycle_iteration->br_area_cnt), idx)){
			idx++;
			continue;
		}

		if (desc == NULL) {
			extern_continue_rebuild(cycle_iteration->iter_idx, idx, cycle_iteration, raid_bdev);
			idx++;
			continue;
		}

		ch = spdk_bdev_get_io_channel(desc);

		cb_arg_new = alloc_cb_arg(cycle_iteration->iter_idx, idx, cycle_iteration, raid_bdev);
		ret = spdk_bdev_writev_blocks(desc, ch,
						  raid_bdev->rebuild->cycle_progress->base_bdevs_sg_buf[info->buf_idx],
						  raid_bdev->rebuild->strips_per_area,
						  info->pd_lba, info->pd_blocks,
						  info->cb, cb_arg_new);

		if (spdk_unlikely(ret != 0)) {
			info->cb(NULL, false, cb_arg_new);
		}
		idx++;
	}

	free(info);
	spdk_bdev_free_io(bdev_io);
}

static int
raid1_submit_rebuild_request(struct raid_bdev *raid_bdev, struct rebuild_progress *cycle_progress, spdk_bdev_io_completion_cb cb)
{
	struct raid_rebuild *rebuild = raid_bdev->rebuild;
	struct rebuild_cycle_iteration *cycle_iter = &(cycle_progress->cycle_iteration);
	struct rebuild_first_stage_cb *cb_arg = calloc(1, sizeof(struct rebuild_first_stage_cb));;
	uint8_t base_idx = 0;
	int ret = 0;
	struct spdk_bdev_desc *desc; /*__base_desc_from_raid_bdev(raid_bdev, idx);*/
    struct spdk_io_channel *ch = NULL; /*spdk_bdev_get_io_channel(desc)*/
	struct raid_base_bdev_info *base_info;
	uint64_t pd_lba, pd_blocks;
	uint8_t idx = 0;

	if (cb_arg == NULL)
	{
		return -ENOMEM;
	}

	pd_lba = get_area_offset(cycle_iter->iter_idx, rebuild->strips_per_area, raid_bdev->strip_size);
	pd_blocks = get_area_size(rebuild->strips_per_area, raid_bdev->strip_size);

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info)
	{
		desc = base_info->desc;
		if (desc != NULL)
		{
			ch = spdk_bdev_get_io_channel(desc);

			if (ch != NULL && !SPDK_TEST_BIT(&(cycle_iter->br_area_cnt), idx))
			{
				break;
			}
		}
		idx++;
	}

	if (idx == raid_bdev->num_base_bdevs)
	{
		SPDK_ERRLOG("No available devices for reading from raid");
		SPDK_SET_BIT(&(raid_bdev->rebuild->rebuild_flag), REBUILD_FLAG_FINISH);
		return -ENODEV;
	}

	if (ch == NULL)
	{
		SPDK_TEST_BIT(fl(rebuild), REBUILD_FLAG_FATAL_ERROR);
		return -EIO;
	}

	cb_arg->cb = cb;
	cb_arg->pd_lba = pd_lba;
	cb_arg->pd_blocks = pd_blocks;
	cb_arg->raid_bdev = raid_bdev;
	cb_arg->cycle_progress = cycle_progress;
	cb_arg->buf_idx = base_idx;

	ret = spdk_bdev_readv_blocks(desc, ch,
						  cycle_progress->base_bdevs_sg_buf[base_idx], 
						  rebuild->strips_per_area, 
						  pd_lba, pd_blocks,
						  raid1_submit_rebuild_second_stage, cb_arg);
	if (ret != 0)
	{
		SPDK_TEST_BIT(fl(rebuild), REBUILD_FLAG_FATAL_ERROR);
		return -EIO;
	}
	return ret;
}

static struct raid_bdev_module g_raid1_module = {
	.level = RAID1,
	.base_bdevs_min = 2,
	.base_bdevs_constraint = {CONSTRAINT_MIN_BASE_BDEVS_OPERATIONAL, 1},
	.memory_domains_supported = true, //false?
	.start = raid1_start,
	.stop = raid1_stop,
	.submit_rw_request = raid1_submit_rw_request,
	.rebuild_request = raid1_submit_rebuild_request,
};
RAID_MODULE_REGISTER(&g_raid1_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_raid1)
