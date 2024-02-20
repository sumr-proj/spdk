/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2019 Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

#include "bdev_raid.h"

#include "spdk/env.h"
#include "spdk/thread.h"
#include "spdk/string.h"
#include "spdk/util.h"
#include "lib/thread/thread_internal.h"

#include "spdk/log.h"
#include "spdk/likely.h"

enum raid5_write_type {
	UNDEFINED = 0,
	READ_MODIFY_WRITE = 1,
	DEFAULT = 2
};

struct raid5_info {
	enum raid5_write_type write_type;
};

struct raid5_stripe_request {
	struct raid_bdev_io *raid_io;

	struct iovec **strip_buffs;

	int* strip_buffs_cnts;

	int strip_buffs_cnt;

	int broken_strip_idx;
};

static inline uint8_t
raid5_parity_strip_index(struct raid_bdev *raid_bdev, uint64_t stripe_index)
{
	return raid_bdev->num_base_bdevs - 1 - stripe_index % raid_bdev->num_base_bdevs;
}

static inline struct iovec *
raid5_get_buffer(size_t iovlen)
{
	struct iovec *buffer;

	buffer = spdk_dma_malloc(sizeof(*buffer), 0, NULL);
	if (buffer == NULL) {
		return NULL;
	}

	buffer->iov_len = iovlen;
	buffer->iov_base = spdk_dma_zmalloc(buffer->iov_len * sizeof(char), 0, NULL);
	if (buffer->iov_base == NULL) {
		spdk_dma_free(buffer);
		return NULL;
	}

	return buffer;
}

static inline void
raid5_free_buffer(struct iovec *buffer)
{
	spdk_dma_free(buffer->iov_base);
	spdk_dma_free(buffer);
}

static inline void
raid5_fill_iovs_with_zeroes(struct iovec *iovs, int iovcnt)
{
	uint64_t *b8;
	size_t len8;

	for (int iovidx = 0; iovidx < iovcnt; ++iovidx) {
		b8 = iovs[iovidx].iov_base;
		len8 = iovs[iovidx].iov_len / 8;
		for (size_t i = 0; i < len8; ++i) {
			b8[i] = 0;
		}
	}
}

static void
raid5_queue_io_wait(struct raid_bdev_io *raid_io, struct spdk_bdev *bdev,
		struct spdk_io_channel *ch, spdk_bdev_io_wait_cb cb_fn, void *cb_arg)
{
	raid_io->waitq_entry.bdev = bdev;
	raid_io->waitq_entry.cb_fn = cb_fn;
	raid_io->waitq_entry.cb_arg = cb_arg;
	spdk_bdev_queue_io_wait(bdev, ch, &raid_io->waitq_entry);
}

static bool
raid5_check_io_boundaries(struct raid_bdev_io *raid_io)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	uint64_t start_strip_idx = bdev_io->u.bdev.offset_blocks >> raid_bdev->strip_size_shift;
	uint64_t end_strip_idx = (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1) >>
							raid_bdev->strip_size_shift;
	
	return (start_strip_idx <= end_strip_idx) &&
			(start_strip_idx / (raid_bdev->num_base_bdevs - 1) ==
			end_strip_idx / (raid_bdev->num_base_bdevs - 1));
}

static inline void
raid5_check_raid_ch(struct raid_bdev_io_channel *raid_ch)
{
	assert(raid_ch != NULL);
	assert(raid_ch->base_channel != NULL);
}

static uint64_t
raid5_stripe_idx(struct spdk_bdev_io *bdev_io, struct raid_bdev *raid_bdev)
{
	uint64_t start_strip_idx = bdev_io->u.bdev.offset_blocks >>
					raid_bdev->strip_size_shift;
	return start_strip_idx / (raid_bdev->num_base_bdevs - 1);
}

static uint64_t
raid5_start_strip_idx(struct spdk_bdev_io *bdev_io, struct raid_bdev *raid_bdev)
{
	uint64_t start_strip_idx;
	uint64_t parity_strip_idx;

	start_strip_idx = bdev_io->u.bdev.offset_blocks >> raid_bdev->strip_size_shift;
	parity_strip_idx = raid5_parity_strip_index(raid_bdev,
			start_strip_idx / (raid_bdev->num_base_bdevs - 1));
	start_strip_idx %= (raid_bdev->num_base_bdevs - 1);
	start_strip_idx += 1 + parity_strip_idx;
	return  start_strip_idx >= raid_bdev->num_base_bdevs ?
			start_strip_idx - raid_bdev->num_base_bdevs :
			start_strip_idx;
}

static uint64_t
raid5_end_strip_idx(struct spdk_bdev_io *bdev_io, struct raid_bdev *raid_bdev)
{
	uint64_t end_strip_idx;
	uint64_t parity_strip_idx;

	end_strip_idx = (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1) >>
			raid_bdev->strip_size_shift;
	parity_strip_idx = raid5_parity_strip_index(raid_bdev,
			end_strip_idx / (raid_bdev->num_base_bdevs - 1));
	end_strip_idx %= (raid_bdev->num_base_bdevs - 1);
	end_strip_idx += 1 + parity_strip_idx;
	return end_strip_idx >= raid_bdev->num_base_bdevs ?
			end_strip_idx - raid_bdev->num_base_bdevs :
			end_strip_idx;
}

static uint64_t
raid5_ofs_blcks(struct spdk_bdev_io *bdev_io, struct raid_bdev *raid_bdev, uint64_t idx)
{
	uint64_t ststrip_idx = raid5_start_strip_idx(bdev_io, raid_bdev);

	if (idx == ststrip_idx) {
		return (((bdev_io->u.bdev.offset_blocks >> raid_bdev->strip_size_shift) /
				(raid_bdev->num_base_bdevs - 1)) << raid_bdev->strip_size_shift) +
				(bdev_io->u.bdev.offset_blocks & (raid_bdev->strip_size - 1));
	} else {
		return ((bdev_io->u.bdev.offset_blocks >> raid_bdev->strip_size_shift) /
				(raid_bdev->num_base_bdevs - 1)) << raid_bdev->strip_size_shift;
	}
}

static uint64_t
raid5_num_blcks(struct spdk_bdev_io *bdev_io, struct raid_bdev *raid_bdev, uint64_t idx)
{
	uint64_t ststrip_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t estrip_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t st_ofs = (bdev_io->u.bdev.offset_blocks & (raid_bdev->strip_size - 1));

	if (idx == ststrip_idx) {
		if (bdev_io->u.bdev.num_blocks + st_ofs <= raid_bdev->strip_size) {
			return bdev_io->u.bdev.num_blocks;
		} else {
			return raid_bdev->strip_size - st_ofs;
		}
	} else if (idx == estrip_idx) {
		return ((bdev_io->u.bdev.num_blocks + st_ofs - 1) &
				(raid_bdev->strip_size - 1)) + 1;
	} else {
		return raid_bdev->strip_size;
	}
}

static inline bool
raid5_is_req_strip(uint64_t ststrip_idx, uint64_t estrip_idx, uint64_t idx) {
	return (ststrip_idx <= estrip_idx) ?
			(ststrip_idx <= idx) && (idx <= estrip_idx) :
			(ststrip_idx <= idx) || (idx <= estrip_idx);
}

static inline uint64_t
raid5_next_idx(uint64_t curr, struct raid_bdev *raid_bdev)
{
	return (curr + 1) >= raid_bdev->num_base_bdevs ?
			curr + 1 - raid_bdev->num_base_bdevs :
			curr + 1;
}

static void
raid5_xor_iovs_with_iovs(struct iovec *xor_iovs, int xor_iovcnt, uint64_t xor_ofs_b8,
				struct iovec *iovs, int iovcnt, uint64_t ofs_b8,
				uint64_t num_b8)
{
	uint64_t *xb8;
	uint64_t *b8;
	uint64_t xofs8 = xor_ofs_b8;
	uint64_t ofs8 = ofs_b8;
	uint64_t xor_idx = 0;
	uint64_t idx = 0;

	while (xofs8 >= xor_iovs[xor_idx].iov_len / 8) {
		xofs8 -= xor_iovs[xor_idx].iov_len / 8;
		++xor_idx;
	}

	while (ofs8 >= iovs[idx].iov_len / 8) {
		ofs8 -= iovs[idx].iov_len / 8;
		++idx;
	}

	while (num_b8 > 0) {
		xb8 = xor_iovs[xor_idx].iov_base;
		xb8 = &xb8[xofs8];
		b8 = iovs[idx].iov_base;
		b8 = &b8[ofs8];
		if (xor_iovs[xor_idx].iov_len / 8 - xofs8 >
				iovs[idx].iov_len / 8 - ofs8) {
			if (num_b8 + ofs8 < iovs[idx].iov_len / 8) {
				for (uint64_t i = 0; i < num_b8; ++i) {
					xb8[i] ^= b8[i];
				}
				num_b8 = 0;
			} else {
				for (uint64_t i = 0; i < (iovs[idx].iov_len / 8) - ofs8; ++i) {
					xb8[i] ^= b8[i];
				}
				num_b8 -= iovs[idx].iov_len / 8 - ofs8;
				xofs8 += iovs[idx].iov_len / 8 - ofs8;
				++idx;
				ofs8 = 0;
			}
		} else if (xor_iovs[xor_idx].iov_len / 8 - xofs8 <
				iovs[idx].iov_len / 8 - ofs8) {
			if (num_b8 + xofs8 < xor_iovs[xor_idx].iov_len / 8) {
				for (uint64_t i = 0; i < num_b8; ++i) {
					xb8[i] ^= b8[i];
				}
				num_b8 = 0;
			} else {
				for (uint64_t i = 0; i < (xor_iovs[xor_idx].iov_len / 8) - xofs8; ++i) {
					xb8[i] ^= b8[i];
				}
				num_b8 -= xor_iovs[xor_idx].iov_len / 8 - xofs8;
				ofs8 += xor_iovs[xor_idx].iov_len / 8 - xofs8;
				++xor_idx;
				xofs8 = 0;
			}
		} else {
			if (num_b8 + ofs8 < iovs[idx].iov_len / 8) {
				for (uint64_t i = 0; i < num_b8; ++i) {
					xb8[i] ^= b8[i];
				}
				num_b8 = 0;
			} else {
				for (uint64_t i = 0; i < (iovs[idx].iov_len / 8)- ofs8; ++i) {
					xb8[i] ^= b8[i];
				}
				num_b8 -= iovs[idx].iov_len / 8 - ofs8;
				++idx;
				ofs8 = 0;
				++xor_idx;
				xofs8 = 0;
			}
		}
	}
}

static struct raid5_stripe_request *
raid5_get_stripe_request(struct raid_bdev_io *raid_io)
{
	struct raid5_stripe_request *request;

	request = spdk_dma_malloc(sizeof(struct raid5_stripe_request), 0, NULL);
	if (request == NULL) {
		return NULL;
	}

	request->raid_io = raid_io;
	request->strip_buffs_cnt = raid_io->raid_bdev->num_base_bdevs;
	request->broken_strip_idx = raid_io->raid_bdev->num_base_bdevs;
	request->strip_buffs = spdk_dma_malloc(sizeof(struct iovec *) * request->strip_buffs_cnt, 0, NULL);
	if (request->strip_buffs == NULL) {
		spdk_dma_free(request);
		return NULL;
	}

	request->strip_buffs_cnts = spdk_dma_zmalloc(sizeof(int) * request->strip_buffs_cnt, 0, NULL);
	if (request->strip_buffs_cnts == NULL) {
		spdk_dma_free(request->strip_buffs);
		spdk_dma_free(request);
		return NULL;
	}

	return request;
}

static void
raid5_free_stripe_request(struct raid5_stripe_request *request) {
	spdk_dma_free(request->strip_buffs_cnts);
	spdk_dma_free(request->strip_buffs);
	spdk_dma_free(request);
}

static int
raid5_allocate_strips_buffs_until(struct raid5_stripe_request *request,
				uint8_t start_idx, uint8_t until_idx, uint64_t num_blcks)
{
	struct raid_bdev *raid_bdev = request->raid_io->raid_bdev;
	uint64_t block_size_b = ((uint64_t)1024 * raid_bdev->strip_size_kb) / raid_bdev->strip_size;

	for (uint8_t idx = start_idx; idx != until_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		request->strip_buffs_cnts[idx] = 1;
		request->strip_buffs[idx] = raid5_get_buffer(num_blcks * block_size_b);
		if (request->strip_buffs[idx] == NULL) {
			for (uint8_t i = start_idx; i != idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_free_buffer(request->strip_buffs[i]);
				request->strip_buffs_cnts[i] = 0;
			}
			request->strip_buffs_cnts[idx] = 0;
			return -ENOMEM;
		}
	}
	return 0;
}

static void
raid5_free_strips_buffs_until(struct raid5_stripe_request *request,
				uint8_t start_idx, uint8_t until_idx)
{
	struct raid_bdev *raid_bdev = request->raid_io->raid_bdev;

	for (uint8_t idx = start_idx; idx != until_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		raid5_free_buffer(request->strip_buffs[idx]);
		request->strip_buffs_cnts[idx] = 0;
	}
}

static int
raid5_set_req_strips_iovs_until(struct raid5_stripe_request *request,
				uint8_t start_idx, uint8_t until_idx,
				int *iov_idx, uint64_t *remaining_len)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	uint64_t			num_blcks;
	uint64_t			len;
	uint64_t			block_size_b = ((uint64_t)1024 * raid_bdev->strip_size_kb) / raid_bdev->strip_size;
	uint64_t			*iov_base_b8;
	int			end_iov_idx;

	for (uint8_t idx = start_idx; idx != until_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, idx);
		end_iov_idx = *iov_idx;
		len = *remaining_len;

		while ((len / block_size_b) < num_blcks) {
			++end_iov_idx;
			len += bdev_io->u.bdev.iovs[end_iov_idx].iov_len;
		}

		len = num_blcks * block_size_b;

		request->strip_buffs_cnts[idx] = end_iov_idx - *iov_idx + 1;
		request->strip_buffs[idx] = spdk_dma_malloc(sizeof(struct iovec) * request->strip_buffs_cnts[idx], 0, NULL);
		if (request->strip_buffs[idx] == NULL) {
			for (uint8_t i = start_idx; i != idx; i = raid5_next_idx(i, raid_bdev)) {
				spdk_dma_free(request->strip_buffs[i]);
				request->strip_buffs_cnts[i] = 0;
			}
			request->strip_buffs_cnts[idx] = 0;
			return -ENOMEM;
		}
		
		iov_base_b8 = bdev_io->u.bdev.iovs[*iov_idx].iov_base;
		request->strip_buffs[idx][0].iov_base =
				&iov_base_b8[(bdev_io->u.bdev.iovs[*iov_idx].iov_len - *remaining_len) / 8];
		if (*remaining_len >= num_blcks * block_size_b) {
			request->strip_buffs[idx][0].iov_len = num_blcks * block_size_b;
			len -= num_blcks * block_size_b;
			*remaining_len -= num_blcks * block_size_b;
		} else {
			request->strip_buffs[idx][0].iov_len = *remaining_len;
			len -= *remaining_len;
			for (uint8_t i = *iov_idx + 1; i < end_iov_idx; ++i) {
				request->strip_buffs[idx][i - *iov_idx].iov_base = bdev_io->u.bdev.iovs[i].iov_base;
				request->strip_buffs[idx][i - *iov_idx].iov_len = bdev_io->u.bdev.iovs[i].iov_len;
				len -= request->strip_buffs[idx][i - *iov_idx].iov_len;
			}
			request->strip_buffs[idx][request->strip_buffs_cnts[idx] - 1].iov_base =
					bdev_io->u.bdev.iovs[end_iov_idx].iov_base;
			request->strip_buffs[idx][request->strip_buffs_cnts[idx] - 1].iov_len = len;
			*remaining_len = bdev_io->u.bdev.iovs[end_iov_idx].iov_len - len;
			*iov_idx = end_iov_idx;
		}

		if (*remaining_len == 0) {
			++(*iov_idx);
			if (*iov_idx < bdev_io->u.bdev.iovcnt) {
				*remaining_len = bdev_io->u.bdev.iovs[*iov_idx].iov_len;
			}
		}
	}
	return 0;	
}

static void
raid5_free_req_strips_iovs_until(struct raid5_stripe_request *request,
				uint8_t start_idx, uint8_t until_idx)
{
	struct raid_bdev *raid_bdev = request->raid_io->raid_bdev;

	for (uint8_t idx = start_idx; idx != until_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		spdk_dma_free(request->strip_buffs[idx]);
		request->strip_buffs[idx] = NULL;
		request->strip_buffs_cnts[idx] = 0;
	}
}

static int
raid5_set_all_req_strips_iovs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			ststrip_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint8_t				after_estrip_idx = raid5_next_idx(raid5_end_strip_idx(bdev_io, raid_bdev), raid_bdev);
	uint64_t			remaining_len = bdev_io->u.bdev.iovs[0].iov_len;
	int			iov_idx = 0;

	return raid5_set_req_strips_iovs_until(request, ststrip_idx, after_estrip_idx, &iov_idx, &remaining_len);
}

static void
raid5_free_all_req_strips_iovs(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	uint8_t			ststrip_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint8_t				after_estrip_idx = raid5_next_idx(raid5_end_strip_idx(bdev_io, raid_bdev), raid_bdev);

	raid5_free_req_strips_iovs_until(request, ststrip_idx, after_estrip_idx);
}

static int
raid5_set_all_strip_buffs(struct raid5_stripe_request *request, uint64_t ofs_blcks, uint64_t num_blcks)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint8_t				after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			remaining_len = bdev_io->u.bdev.iovs[0].iov_len;
	uint64_t			len;
	uint64_t			block_size_b = ((uint64_t)1024 * raid_bdev->strip_size_kb) / raid_bdev->strip_size;
	uint64_t			*iov_base_b8;
	uint64_t			blocks;
	int			end_iov_idx;
	int			iov_idx = 0;
	int			ret = 0;
	int			sts_idx_ofs = 0;
	int			es_idx_extra = 0;

	// not req strip and parity strip
	ret = raid5_allocate_strips_buffs_until(request, after_es_idx, sts_idx, num_blcks);
	if (ret != 0) {
		return ret;
	}

	// start req strip
	sts_idx_ofs = ofs_blcks != raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx) ?
					1 : 0;

	blocks = raid5_num_blcks(bdev_io, raid_bdev, sts_idx);
	end_iov_idx = iov_idx;
	len = remaining_len;

	while ((len / block_size_b) < blocks) {
		++end_iov_idx;
		len += bdev_io->u.bdev.iovs[end_iov_idx].iov_len;
	}

	request->strip_buffs_cnts[sts_idx] = end_iov_idx - iov_idx + 1 + sts_idx_ofs;
	request->strip_buffs[sts_idx] = spdk_dma_malloc(sizeof(struct iovec) * request->strip_buffs_cnts[sts_idx], 0, NULL);
	if (request->strip_buffs[sts_idx] == NULL) {
		raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
		request->strip_buffs_cnts[sts_idx] = 0;
		return -ENOMEM;
	}
	
	len = blocks * block_size_b;
	
	iov_base_b8 = bdev_io->u.bdev.iovs[iov_idx].iov_base;
	request->strip_buffs[sts_idx][sts_idx_ofs].iov_base =
			&iov_base_b8[(bdev_io->u.bdev.iovs[iov_idx].iov_len - remaining_len) / 8];

	if (remaining_len >= blocks * block_size_b) {
		request->strip_buffs[sts_idx][sts_idx_ofs].iov_len = blocks * block_size_b;
		len -= blocks * block_size_b;
		remaining_len -= blocks * block_size_b;
	} else {
		request->strip_buffs[sts_idx][sts_idx_ofs].iov_len = remaining_len;
		len -= remaining_len;
		for (uint8_t i = iov_idx + 1; i < end_iov_idx; ++i) {
			request->strip_buffs[sts_idx][sts_idx_ofs + i - iov_idx].iov_base = bdev_io->u.bdev.iovs[i].iov_base;
			request->strip_buffs[sts_idx][sts_idx_ofs + i - iov_idx].iov_len = bdev_io->u.bdev.iovs[i].iov_len;
			len -= request->strip_buffs[sts_idx][sts_idx_ofs + i - iov_idx].iov_len;
		}
		request->strip_buffs[sts_idx][request->strip_buffs_cnts[sts_idx] - 1].iov_base =
				bdev_io->u.bdev.iovs[end_iov_idx].iov_base;
		request->strip_buffs[sts_idx][request->strip_buffs_cnts[sts_idx] - 1].iov_len = len;
		remaining_len = bdev_io->u.bdev.iovs[end_iov_idx].iov_len - len;
		iov_idx = end_iov_idx;
	}

	if (remaining_len == 0) {
		++iov_idx;
		if (iov_idx < bdev_io->u.bdev.iovcnt) {
			remaining_len = bdev_io->u.bdev.iovs[iov_idx].iov_len;
		}
	}

	if (sts_idx_ofs == 1) {
		request->strip_buffs[sts_idx][0].iov_len = (raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx)
						- ofs_blcks) * block_size_b;
		request->strip_buffs[sts_idx][0].iov_base = spdk_dma_zmalloc(sizeof(char) *
								request->strip_buffs[sts_idx][0].iov_len, 0, NULL);
		if (request->strip_buffs[sts_idx][0].iov_base == NULL) {
			raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
			spdk_dma_free(request->strip_buffs[sts_idx]);
			request->strip_buffs[sts_idx] = NULL;
			request->strip_buffs_cnts[sts_idx] = 0;
			return -ENOMEM;
		}
	}

	if (sts_idx == es_idx) {
		return 0;
	}

	// middle req strip
	ret = raid5_set_req_strips_iovs_until(request,
					raid5_next_idx(sts_idx, raid_bdev), es_idx,
					&iov_idx, &remaining_len);
	if (ret != 0) {
		raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
		if (sts_idx_ofs == 1) {
			spdk_dma_free(request->strip_buffs[sts_idx][0].iov_base);
		}
		spdk_dma_free(request->strip_buffs[sts_idx]);
		request->strip_buffs[sts_idx] = NULL;
		request->strip_buffs_cnts[sts_idx] = 0;
		
		return ret;
	}

	// end req strip
	es_idx_extra = ofs_blcks + num_blcks >
			raid5_ofs_blcks(bdev_io, raid_bdev, es_idx) +
			raid5_num_blcks(bdev_io, raid_bdev, es_idx) ?
			1 : 0;

	blocks = raid5_num_blcks(bdev_io, raid_bdev, es_idx);
	end_iov_idx = iov_idx;
	len = remaining_len;

	while ((len / block_size_b) < blocks) {
		++end_iov_idx;
		len += bdev_io->u.bdev.iovs[end_iov_idx].iov_len;
	}

	request->strip_buffs_cnts[es_idx] = end_iov_idx - iov_idx + 1 + es_idx_extra;
	request->strip_buffs[es_idx] = spdk_dma_malloc(sizeof(struct iovec) * request->strip_buffs_cnts[es_idx], 0, NULL);
	if (request->strip_buffs[es_idx] == NULL) {
		raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
		if (sts_idx_ofs == 1) {
			spdk_dma_free(request->strip_buffs[sts_idx][0].iov_base);
		}
		spdk_dma_free(request->strip_buffs[sts_idx]);
		request->strip_buffs[sts_idx] = NULL;
		request->strip_buffs_cnts[sts_idx] = 0;
		raid5_free_req_strips_iovs_until(request,
						raid5_next_idx(sts_idx, raid_bdev), es_idx);
		request->strip_buffs_cnts[es_idx] = 0;
		return -ENOMEM;
	}

	len = blocks * block_size_b;

	iov_base_b8 = bdev_io->u.bdev.iovs[iov_idx].iov_base;
	request->strip_buffs[es_idx][0].iov_base =
			&iov_base_b8[(bdev_io->u.bdev.iovs[iov_idx].iov_len - remaining_len) / 8];
	if (remaining_len >= blocks * block_size_b) {
		request->strip_buffs[es_idx][0].iov_len = blocks * block_size_b;
		len -= blocks * block_size_b;
		remaining_len -= blocks * block_size_b;
	} else {
		request->strip_buffs[es_idx][0].iov_len = remaining_len;
		len -= remaining_len;
		for (uint8_t i = iov_idx + 1; i < end_iov_idx; ++i) {
			request->strip_buffs[es_idx][i - iov_idx].iov_base = bdev_io->u.bdev.iovs[i].iov_base;
			request->strip_buffs[es_idx][i - iov_idx].iov_len = bdev_io->u.bdev.iovs[i].iov_len;
			len -= request->strip_buffs[es_idx][i - iov_idx].iov_len;
		}
		request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx] - 1].iov_base =
				bdev_io->u.bdev.iovs[end_iov_idx].iov_base;
		request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx] - 1].iov_len = len;
		remaining_len = bdev_io->u.bdev.iovs[end_iov_idx].iov_len - len;
		iov_idx = end_iov_idx;
	}

	if (remaining_len == 0) {
		++iov_idx;
		if (iov_idx < bdev_io->u.bdev.iovcnt) {
			remaining_len = bdev_io->u.bdev.iovs[iov_idx].iov_len;
		}
	}

	if (es_idx_extra == 1) {
		request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx] - 1].iov_len =
						(ofs_blcks + num_blcks -
						(raid5_ofs_blcks(bdev_io, raid_bdev, es_idx) +
						raid5_num_blcks(bdev_io, raid_bdev, es_idx))) *
						block_size_b;
		request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx] - 1].iov_base =
						spdk_dma_zmalloc(sizeof(char) *
							request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx]
							- 1].iov_len, 0 , NULL);
		if (request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx] - 1].iov_base
				== NULL) {
			raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
			if (sts_idx_ofs == 1) {
				spdk_dma_free(request->strip_buffs[sts_idx][0].iov_base);
			}
			spdk_dma_free(request->strip_buffs[sts_idx]);
			request->strip_buffs[sts_idx] = NULL;
			request->strip_buffs_cnts[sts_idx] = 0;
			raid5_free_req_strips_iovs_until(request,
							raid5_next_idx(sts_idx, raid_bdev), es_idx);
			spdk_dma_free(request->strip_buffs[es_idx]);
			request->strip_buffs[es_idx] = NULL;
			request->strip_buffs_cnts[es_idx] = 0;
			return -ENOMEM;
		}
	}
	return 0;
}

static void
raid5_free_all_strip_buffs(struct raid5_stripe_request *request, uint64_t ofs_blcks, uint64_t num_blcks)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint8_t				after_es_idx = raid5_next_idx(es_idx, raid_bdev);

	raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);

	if (ofs_blcks != raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx)) {
		spdk_dma_free(request->strip_buffs[sts_idx][0].iov_base);
	}
	spdk_dma_free(request->strip_buffs[sts_idx]);
	request->strip_buffs[sts_idx] = NULL;
	request->strip_buffs_cnts[sts_idx] = 0;
	if (sts_idx == es_idx) {
		return;
	}

	raid5_free_req_strips_iovs_until(request,
					raid5_next_idx(sts_idx, raid_bdev), es_idx);

	if (ofs_blcks + num_blcks > raid5_ofs_blcks(bdev_io, raid_bdev, es_idx) +
			raid5_num_blcks(bdev_io, raid_bdev, es_idx)) {
		spdk_dma_free(request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx]
				- 1].iov_base);
	}
	spdk_dma_free(request->strip_buffs[es_idx]);
	request->strip_buffs[es_idx] = NULL;
	request->strip_buffs_cnts[es_idx] = 0;
}

static int
raid5_read_req_strips_set_strip_buffs(struct raid5_stripe_request *request)
{
	return raid5_set_all_req_strips_iovs(request);
}

static void
raid5_read_req_strips_free_strip_buffs(struct raid5_stripe_request *request)
{
	raid5_free_all_req_strips_iovs(request);
}

static int
raid5_read_exc_req_strip_set_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, request->broken_strip_idx);
	uint64_t			num_blcks = raid5_num_blcks(bdev_io, raid_bdev, request->broken_strip_idx);

	return raid5_set_all_strip_buffs(request, ofs_blcks, num_blcks);
}

static void
raid5_read_exc_req_strip_free_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, request->broken_strip_idx);
	uint64_t			num_blcks = raid5_num_blcks(bdev_io, raid_bdev, request->broken_strip_idx);

	raid5_free_all_strip_buffs(request, ofs_blcks, num_blcks);
}

static int
raid5_write_default_set_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
	uint64_t			es_num_blcks = raid5_num_blcks(bdev_io, raid_bdev, es_idx);

	if (sts_idx != es_idx) {
		return raid5_set_all_strip_buffs(request, es_ofs_blcks, raid_bdev->strip_size);
	} else {
		return raid5_set_all_strip_buffs(request, es_ofs_blcks, es_num_blcks);
	}
}

static void
raid5_write_default_free_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
	uint64_t			es_num_blcks = raid5_num_blcks(bdev_io, raid_bdev, es_idx);

	if (sts_idx != es_idx) {
		return raid5_free_all_strip_buffs(request, es_ofs_blcks, raid_bdev->strip_size);
	} else {
		return raid5_free_all_strip_buffs(request, es_ofs_blcks, es_num_blcks);
	}
}

static int
raid5_write_broken_parity_set_strip_buffs(struct raid5_stripe_request *request)
{
	return raid5_set_all_req_strips_iovs(request);
}

static void
raid5_write_broken_parity_free_strip_buffs(struct raid5_stripe_request *request)
{
	raid5_free_all_req_strips_iovs(request);
}

static int
raid5_write_r_modify_w_set_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			sts_num_blcks = raid5_num_blcks(bdev_io, raid_bdev, sts_idx);
	uint64_t			after_sts_idx = raid5_next_idx(sts_idx, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_num_blcks = raid5_num_blcks(bdev_io, raid_bdev, es_idx);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			after_ps_idx = raid5_next_idx(ps_idx, raid_bdev);
	int				ret = 0;

	ret = raid5_allocate_strips_buffs_until(request, sts_idx, after_sts_idx, sts_num_blcks);
	if (ret != 0) {
		return ret;
	}

	if (sts_idx == es_idx) {
		ret = raid5_allocate_strips_buffs_until(request, ps_idx, after_ps_idx, sts_num_blcks);
		if (ret != 0) {
			raid5_free_strips_buffs_until(request, sts_idx, after_sts_idx);
		}
		return ret;
	} else {
		ret = raid5_allocate_strips_buffs_until(request, ps_idx, after_ps_idx, raid_bdev->strip_size);
		if (ret != 0) {
			raid5_free_strips_buffs_until(request, sts_idx, after_sts_idx);
			return ret;
		}
	}

	ret = raid5_allocate_strips_buffs_until(request, after_sts_idx, es_idx, raid_bdev->strip_size);
	if (ret != 0) {
		raid5_free_strips_buffs_until(request, sts_idx, after_sts_idx);
		raid5_free_strips_buffs_until(request, ps_idx, after_ps_idx);
		return ret;
	}

	ret = raid5_allocate_strips_buffs_until(request, es_idx, after_es_idx, es_num_blcks);
	if (ret != 0) {
		raid5_free_strips_buffs_until(request, sts_idx, es_idx);
		raid5_free_strips_buffs_until(request, ps_idx, after_ps_idx);
	}
	return ret;
}

static void
raid5_w_r_modify_w_reading_free_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(raid5_end_strip_idx(bdev_io, raid_bdev), raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			after_ps_idx = raid5_next_idx(ps_idx, raid_bdev);

	raid5_free_strips_buffs_until(request, ps_idx, after_ps_idx);
	raid5_free_strips_buffs_until(request, sts_idx, after_es_idx);
}

static int
raid5_write_r_modify_w_reset_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(raid5_end_strip_idx(bdev_io, raid_bdev), raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			after_ps_idx = raid5_next_idx(ps_idx, raid_bdev);
	int				ret = 0;

	raid5_free_strips_buffs_until(request, sts_idx, after_es_idx);
	ret = raid5_set_all_req_strips_iovs(request);
	if (ret != 0) {
		raid5_free_strips_buffs_until(request, ps_idx, after_ps_idx);
	}
	return ret;
}

static void
raid5_w_r_modify_w_writing_free_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			after_ps_idx = raid5_next_idx(ps_idx, raid_bdev);

	raid5_free_strips_buffs_until(request, ps_idx, after_ps_idx);
	raid5_free_all_req_strips_iovs(request);
}

static int
raid5_write_broken_req_set_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_sts_idx = raid5_next_idx(sts_idx, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	
	if (sts_idx == es_idx) {
		return raid5_allocate_strips_buffs_until(request, after_sts_idx, sts_idx,
					raid5_num_blcks(bdev_io, raid_bdev, sts_idx));
	} else if (request->broken_strip_idx != sts_idx && request->broken_strip_idx != es_idx) {
		return raid5_allocate_strips_buffs_until(request, es_idx, after_sts_idx, raid_bdev->strip_size);
	} else if (request->broken_strip_idx == sts_idx) {
		return raid5_allocate_strips_buffs_until(request, after_sts_idx, sts_idx, raid_bdev->strip_size);
	} else {
		return raid5_allocate_strips_buffs_until(request, after_es_idx, es_idx, raid_bdev->strip_size);
	}
}

static void
raid5_w_br_r_reading_free_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_sts_idx = raid5_next_idx(sts_idx, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	
	if (sts_idx == es_idx) {
		raid5_free_strips_buffs_until(request, after_sts_idx, sts_idx);
	} else if (request->broken_strip_idx != sts_idx && request->broken_strip_idx != es_idx) {
		raid5_free_strips_buffs_until(request, es_idx, after_sts_idx);
	} else if (request->broken_strip_idx == sts_idx) {
		raid5_free_strips_buffs_until(request, after_sts_idx, sts_idx);
	} else {
		raid5_free_strips_buffs_until(request, after_es_idx, es_idx);
	}
}

static int
raid5_write_broken_req_reset_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_sts_idx = raid5_next_idx(sts_idx, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			after_ps_idx = raid5_next_idx(ps_idx, raid_bdev);
	int				ret = 0;
	
	if (sts_idx == es_idx) {
		raid5_free_strips_buffs_until(request, after_sts_idx, ps_idx);
		raid5_free_strips_buffs_until(request, after_ps_idx, sts_idx);
	} else if (request->broken_strip_idx != sts_idx && request->broken_strip_idx != es_idx) {
		raid5_free_strips_buffs_until(request, es_idx, ps_idx);
		raid5_free_strips_buffs_until(request, after_ps_idx, after_sts_idx);
	} else if (request->broken_strip_idx == sts_idx) {
		raid5_free_strips_buffs_until(request, after_sts_idx, ps_idx);
		raid5_free_strips_buffs_until(request, after_ps_idx, sts_idx);
	} else {
		raid5_free_strips_buffs_until(request, after_es_idx, ps_idx);
		raid5_free_strips_buffs_until(request, after_ps_idx, es_idx);
	}
	ret = raid5_set_all_req_strips_iovs(request);
	if (ret != 0) {
		raid5_free_strips_buffs_until(request, ps_idx, after_ps_idx);
	}
	return ret;
}

static void
raid5_w_br_r_writing_free_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			after_ps_idx = raid5_next_idx(ps_idx, raid_bdev);

	raid5_free_strips_buffs_until(request, ps_idx, after_ps_idx);
	raid5_free_all_req_strips_iovs(request);
}

static void
raid5_stripe_req_complete(struct raid5_stripe_request *request)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	raid5_free_stripe_request(request);

	raid_bdev_io_complete(raid_io, raid_io->base_bdev_io_status);	
}

static bool
raid5_read_complete_part(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		if (request->broken_strip_idx != raid_io->raid_bdev->num_base_bdevs) {
			struct raid_bdev 		*raid_bdev = raid_io->raid_bdev;
			struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(raid_io);
			uint64_t block_size_b8 = ((uint64_t)128 * raid_bdev->strip_size_kb)
					/ raid_bdev->strip_size;
			uint64_t br_ofs_b8 = block_size_b8 * raid5_ofs_blcks(bdev_io, raid_bdev, request->broken_strip_idx);
			uint64_t br_num_b8 = block_size_b8 * raid5_num_blcks(bdev_io, raid_bdev, request->broken_strip_idx);
			uint64_t ofs_b8;
			uint8_t sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
			uint8_t es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
			uint8_t after_es_idx = raid5_next_idx(es_idx, raid_bdev);
			uint8_t after_broken = raid5_next_idx(request->broken_strip_idx, raid_bdev);
			raid5_fill_iovs_with_zeroes(request->strip_buffs[request->broken_strip_idx],
							request->strip_buffs_cnts[request->broken_strip_idx]);

			for (uint8_t i = after_es_idx; i != sts_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[request->broken_strip_idx],
						request->strip_buffs_cnts[request->broken_strip_idx], 0,
						request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
						br_num_b8);
			}

			for (uint8_t i = sts_idx; i != request->broken_strip_idx; i = raid5_next_idx(i, raid_bdev)) {
				ofs_b8 = block_size_b8 * raid5_ofs_blcks(bdev_io, raid_bdev, i);
				if (br_ofs_b8 >= ofs_b8) {
					ofs_b8 = br_ofs_b8 - ofs_b8;
				} else {
					ofs_b8 = 0;
				}
				raid5_xor_iovs_with_iovs(request->strip_buffs[request->broken_strip_idx],
						request->strip_buffs_cnts[request->broken_strip_idx], 0,
						request->strip_buffs[i], request->strip_buffs_cnts[i], ofs_b8,
						br_num_b8);
			}

			for (uint8_t i = after_broken; i != after_es_idx; i = raid5_next_idx(i, raid_bdev)) {
				ofs_b8 = block_size_b8 * raid5_ofs_blcks(bdev_io, raid_bdev, i);
				if (br_ofs_b8 >= ofs_b8) {
					ofs_b8 = br_ofs_b8 - ofs_b8;
				} else {
					ofs_b8 = 0;
				}
				raid5_xor_iovs_with_iovs(request->strip_buffs[request->broken_strip_idx],
						request->strip_buffs_cnts[request->broken_strip_idx], 0,
						request->strip_buffs[i], request->strip_buffs_cnts[i], ofs_b8,
						br_num_b8);
			}

			raid5_read_exc_req_strip_free_strip_buffs(request);
		} else {
			raid5_read_req_strips_free_strip_buffs(request);
		}

		raid5_stripe_req_complete(request);
		return true;
	} else {
		return false;
	}
}

static void raid5_read_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5_read_complete_part(request, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid5_read_req_strips(struct raid5_stripe_request *request);

static void
_raid5_read_req_strips(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_read_req_strips(request);
}

static void
raid5_read_req_strips(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			ststrip_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			estrip_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			base_bdev_io_not_submitted;
	uint8_t				after_estrip_idx = raid5_next_idx(estrip_idx, raid_bdev);
	uint8_t				start_idx;
	int				ret = 0;

	start_idx = (ststrip_idx + raid_io->base_bdev_io_submitted) > raid_bdev->num_base_bdevs ?
				ststrip_idx + raid_io->base_bdev_io_submitted - raid_bdev->num_base_bdevs :
				ststrip_idx + raid_io->base_bdev_io_submitted;

	for (uint8_t idx = start_idx; idx != after_estrip_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_ch->base_channel[idx];

		ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
											request->strip_buffs[idx], request->strip_buffs_cnts[idx],
											raid5_ofs_blcks(bdev_io, raid_bdev, idx),
											raid5_num_blcks(bdev_io, raid_bdev, idx),
											raid5_read_cb,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_read_req_strips, request);
				return;
			}

			base_bdev_io_not_submitted = ((estrip_idx + raid_bdev->num_base_bdevs) -
							ststrip_idx) % raid_bdev->num_base_bdevs + 1 -
							raid_io->base_bdev_io_submitted;
			raid5_read_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		++raid_io->base_bdev_io_submitted;
	}
}

static void raid5_read_except_one_req_strip(struct raid5_stripe_request *request);

static void
_raid5_read_except_one_req_strip(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_read_except_one_req_strip(request);
}

static void
raid5_read_except_one_req_strip(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			ststrip_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			estrip_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			base_bdev_io_not_submitted;
	uint64_t			ofs_blcks;
	uint64_t			num_blcks;
	uint8_t				after_brstrip_idx = raid5_next_idx(request->broken_strip_idx, raid_bdev);
	uint8_t				start_idx;
	int				ret = 0;

	start_idx = (after_brstrip_idx + raid_io->base_bdev_io_submitted) > raid_bdev->num_base_bdevs ?
				after_brstrip_idx + raid_io->base_bdev_io_submitted - raid_bdev->num_base_bdevs :
				after_brstrip_idx + raid_io->base_bdev_io_submitted;
	
	for (uint8_t idx = start_idx; idx != request->broken_strip_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_ch->base_channel[idx];
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, request->broken_strip_idx);
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, request->broken_strip_idx);

		if (raid5_is_req_strip(ststrip_idx, estrip_idx, idx)) {
			if (ofs_blcks != raid5_ofs_blcks(bdev_io, raid_bdev, idx)) {
				num_blcks = raid_bdev->strip_size;
				ofs_blcks = spdk_min(ofs_blcks, raid5_ofs_blcks(bdev_io, raid_bdev, idx));
			} else {
				num_blcks = spdk_max(num_blcks, raid5_num_blcks(bdev_io, raid_bdev, idx));
			}
		}

		ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
											request->strip_buffs[idx], request->strip_buffs_cnts[idx],
											ofs_blcks, num_blcks,
											raid5_read_cb,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_read_except_one_req_strip, request);
				return;
			}

			base_bdev_io_not_submitted = raid_bdev->num_base_bdevs - 1 -
							raid_io->base_bdev_io_submitted;
			raid5_read_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		++raid_io->base_bdev_io_submitted;
	}
}

static void
raid5_submit_read_request(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct spdk_io_channel		*base_ch;
	uint64_t			ststrip_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			estrip_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint8_t				after_estrip_idx = raid5_next_idx(estrip_idx, raid_bdev);
	int				ret = 0;

	for (uint8_t idx = ststrip_idx; idx != after_estrip_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		base_ch = raid_ch->base_channel[idx];

		if (base_ch == NULL) {
			if (request->broken_strip_idx == raid_bdev->num_base_bdevs) {
				request->broken_strip_idx = idx;
			} else {
				SPDK_ERRLOG("RAID5 read request: 2 broken strips\n");
				raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
				raid5_stripe_req_complete(request);
				assert(false);
				return;
			}
		}
	}

	if (request->broken_strip_idx == raid_bdev->num_base_bdevs) {
		ret = raid5_read_req_strips_set_strip_buffs(request);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("RAID5 read request: allocation of buffers is failed\n");
			raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
			raid5_stripe_req_complete(request);
			return;
		}

		raid_io->base_bdev_io_submitted = 0;
		raid_io->base_bdev_io_remaining = ((estrip_idx + raid_bdev->num_base_bdevs) -
				ststrip_idx) % raid_bdev->num_base_bdevs + 1;
		raid5_read_req_strips(request);
	} else {
		for (uint8_t idx = after_estrip_idx; idx != ststrip_idx; idx = raid5_next_idx(idx, raid_bdev)) {
			base_ch = raid_ch->base_channel[idx];

			if (base_ch == NULL) {
				SPDK_ERRLOG("RAID5 read request: 2 broken strips\n");
				raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
				raid5_stripe_req_complete(request);
				assert(false);
				return;
			}
		}

		ret = raid5_read_exc_req_strip_set_strip_buffs(request);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("RAID5 read request: allocation of buffers is failed\n");
			raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
			raid5_stripe_req_complete(request);
			return;
		}

		raid_io->base_bdev_io_submitted = 0;
		raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs - 1;
		raid5_read_except_one_req_strip(request);
	}
}

static bool
raid5_w_default_writing_complete_part(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		raid5_write_default_free_strip_buffs(request);
		raid5_stripe_req_complete(request);

		return true;
	} else {
		return false;
	}
}

static void
raid5_w_default_writing_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5_w_default_writing_complete_part(request, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid5_write_default_writing(struct raid5_stripe_request *request);

static void
_raid5_write_default_writing(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_write_default_writing(request);
}

static void
raid5_write_default_writing(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			num_strips_to_w = (((es_idx + raid_bdev->num_base_bdevs) -
								sts_idx) % raid_bdev->num_base_bdevs) + 2;
	uint64_t			base_bdev_io_not_submitted;
	uint64_t			ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
	uint64_t			num_blcks;
	uint8_t				start_idx;
	int				ret = 0;

	if (es_idx == sts_idx) {
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, sts_idx);
	} else {
		num_blcks = raid_bdev->strip_size;
	}

	if (raid_io->base_bdev_io_submitted == 0) {
		base_info = &raid_bdev->base_bdev_info[ps_idx];
		base_ch = raid_ch->base_channel[ps_idx];

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
										request->strip_buffs[ps_idx], request->strip_buffs_cnts[ps_idx],
										ofs_blcks, num_blcks,
										raid5_w_default_writing_cb,
										request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_default_writing, request);
				return;
			}

			base_bdev_io_not_submitted = num_strips_to_w - raid_io->base_bdev_io_submitted;
			raid5_w_default_writing_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
		++raid_io->base_bdev_io_submitted;
	}
	start_idx = (sts_idx + raid_io->base_bdev_io_submitted - 1) % raid_bdev->num_base_bdevs;

	for (uint8_t idx = start_idx; idx != after_es_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_ch->base_channel[idx];

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
											request->strip_buffs[idx], request->strip_buffs_cnts[idx],
											ofs_blcks, num_blcks,
											raid5_w_default_writing_cb,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_default_writing, request);
				return;
			}

			base_bdev_io_not_submitted = num_strips_to_w - raid_io->base_bdev_io_submitted;
			raid5_w_default_writing_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		++raid_io->base_bdev_io_submitted;
	}
}

static bool
raid5_w_default_reading_complete_part(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		struct raid_bdev 		*raid_bdev = raid_io->raid_bdev;
		struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(raid_io);
		uint64_t block_size_b8 = ((uint64_t)128 * raid_bdev->strip_size_kb) /
					raid_bdev->strip_size;
		uint8_t sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
		uint8_t es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
		uint8_t ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
		uint8_t after_ps_idx = raid5_next_idx(ps_idx, raid_bdev);
		uint64_t num_b8;

		if (raid_io->base_bdev_io_status != SPDK_BDEV_IO_STATUS_SUCCESS) {
			raid5_write_default_free_strip_buffs(request);
			raid5_stripe_req_complete(request);
			return true;
		}

		if (sts_idx != es_idx) {
			num_b8 = raid_bdev->strip_size * block_size_b8;
		} else {
			num_b8 = raid5_num_blcks(bdev_io, raid_bdev, sts_idx) * block_size_b8;
		}

		for (uint8_t i = sts_idx; i != ps_idx; i = raid5_next_idx(i, raid_bdev)) {
			raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
					request->strip_buffs_cnts[ps_idx], 0,
					request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
					num_b8);
		}

		for (uint8_t i = after_ps_idx; i != sts_idx; i = raid5_next_idx(i, raid_bdev)) {
			raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
					request->strip_buffs_cnts[ps_idx], 0,
					request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
					num_b8);
		}

		raid_io->base_bdev_io_submitted = 0;
		raid_io->base_bdev_io_remaining = (((es_idx + raid_bdev->num_base_bdevs) -
					sts_idx) % raid_bdev->num_base_bdevs) + 2;

		raid5_write_default_writing(request);

		return true;
	} else {
		return false;
	}
}

static void
raid5_w_default_reading_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5_w_default_reading_complete_part(request, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid5_write_default_reading(struct raid5_stripe_request *request);

static void
_raid5_write_default_reading(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_write_default_reading(request);
}

static void
raid5_write_default_reading(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			sts_ofs = raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_ofs = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
	uint64_t			es_num_blcks = raid5_num_blcks(bdev_io, raid_bdev, es_idx);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			num_strips_to_r;
	uint64_t			base_bdev_io_not_submitted;
	uint64_t			ofs_blcks;
	uint64_t			num_blcks;
	uint8_t				start_idx;
	int				ret = 0;

	if (sts_idx != es_idx) {
		num_strips_to_r = raid_bdev->num_base_bdevs - (((es_idx + raid_bdev->num_base_bdevs) -
					sts_idx) % raid_bdev->num_base_bdevs);
		if (raid_io->base_bdev_io_submitted == 0) {
			if (sts_ofs > es_ofs) {
				base_info = &raid_bdev->base_bdev_info[sts_idx];
				base_ch = raid_ch->base_channel[sts_idx];

				ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
												&request->strip_buffs[sts_idx][0], 1,
												es_ofs, sts_ofs - es_ofs,
												raid5_w_default_reading_cb,
												request);
				if (spdk_unlikely(ret != 0)) {
					if (spdk_unlikely(ret == -ENOMEM)) {
						raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
									base_ch, _raid5_write_default_reading, request);
						return;
					}

					base_bdev_io_not_submitted = num_strips_to_r - raid_io->base_bdev_io_submitted;
					raid5_w_default_reading_complete_part(request, base_bdev_io_not_submitted,
									SPDK_BDEV_IO_STATUS_FAILED);
					return;
				}
			} else {
				raid5_w_default_reading_complete_part(request, 1,
								SPDK_BDEV_IO_STATUS_SUCCESS);
			}
			++raid_io->base_bdev_io_submitted;
		}
		if (raid_io->base_bdev_io_submitted == 1) {
			if (raid_bdev->strip_size > es_num_blcks) {
				base_info = &raid_bdev->base_bdev_info[es_idx];
				base_ch = raid_ch->base_channel[es_idx];

				ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
												&request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx] - 1], 1,
												es_ofs + es_num_blcks, raid_bdev->strip_size - es_num_blcks,
												raid5_w_default_reading_cb,
												request);
				if (spdk_unlikely(ret != 0)) {
					if (spdk_unlikely(ret == -ENOMEM)) {
						raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
									base_ch, _raid5_write_default_reading, request);
						return;
					}

					base_bdev_io_not_submitted = num_strips_to_r - raid_io->base_bdev_io_submitted;
					raid5_w_default_reading_complete_part(request, base_bdev_io_not_submitted,
									SPDK_BDEV_IO_STATUS_FAILED);
					return;
				}
			} else {
				raid5_w_default_reading_complete_part(request, 1,
								SPDK_BDEV_IO_STATUS_SUCCESS);
			}
			++raid_io->base_bdev_io_submitted;
		}
		start_idx = (after_es_idx + raid_io->base_bdev_io_submitted - 2) % raid_bdev->num_base_bdevs;
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
		num_blcks = raid_bdev->strip_size;
	} else {
		num_strips_to_r = raid_bdev->num_base_bdevs - (((es_idx + raid_bdev->num_base_bdevs) -
				sts_idx) % raid_bdev->num_base_bdevs) - 2;
		start_idx = (after_es_idx + raid_io->base_bdev_io_submitted) % raid_bdev->num_base_bdevs;
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, es_idx);
	}

	if (start_idx == ps_idx) {
		start_idx = raid5_next_idx(start_idx, raid_bdev);
	}

	for (uint8_t idx = start_idx; idx != sts_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		if (idx == ps_idx) {
			continue;
		}
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_ch->base_channel[idx];

		ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
											request->strip_buffs[idx], request->strip_buffs_cnts[idx],
											ofs_blcks, num_blcks,
											raid5_w_default_reading_cb,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_default_reading, request);
				return;
			}

			base_bdev_io_not_submitted = num_strips_to_r - raid_io->base_bdev_io_submitted;
			raid5_w_default_reading_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		++raid_io->base_bdev_io_submitted;
	}
}

static bool
raid5_w_broken_ps_complete_part(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		raid5_write_broken_parity_free_strip_buffs(request);
		raid5_stripe_req_complete(request);

		return true;
	} else {
		return false;
	}
}

static void
raid5_w_broken_ps_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5_w_broken_ps_complete_part(request, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid5_write_broken_parity_strip(struct raid5_stripe_request *request);

static void
_raid5_write_broken_parity_strip(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_write_broken_parity_strip(request);
}

static void
raid5_write_broken_parity_strip(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			base_bdev_io_not_submitted;
	uint64_t			ofs_blcks;
	uint64_t			num_blcks;
	uint8_t				start_idx;
	int				ret = 0;

	start_idx = (sts_idx + raid_io->base_bdev_io_submitted) % raid_bdev->num_base_bdevs;

	for (uint8_t idx = start_idx; idx != after_es_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_ch->base_channel[idx];

		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, idx);
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, idx);

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
											request->strip_buffs[idx], request->strip_buffs_cnts[idx],
											ofs_blcks, num_blcks,
											raid5_w_broken_ps_cb,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_broken_parity_strip, request);
				return;
			}

			base_bdev_io_not_submitted = ((es_idx + raid_bdev->num_base_bdevs) -
					sts_idx) % raid_bdev->num_base_bdevs + 1 - raid_io->base_bdev_io_submitted;
			raid5_w_broken_ps_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		++raid_io->base_bdev_io_submitted;
	}
}

static bool
raid5_w_r_modify_w_writing_complete_part(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		raid5_w_r_modify_w_writing_free_strip_buffs(request);
		raid5_stripe_req_complete(request);

		return true;
	} else {
		return false;
	}
}

static void
raid5_w_r_modify_w_writing_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5_w_r_modify_w_writing_complete_part(request, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid5_write_r_modify_w_writing(struct raid5_stripe_request *request);

static void
_raid5_write_r_modify_w_writing(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_write_r_modify_w_writing(request);
}

static void
raid5_write_r_modify_w_writing(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			base_bdev_io_not_submitted;
	uint64_t			ofs_blcks;
	uint64_t			num_blcks;
	uint8_t				start_idx;
	int				ret = 0;

	if (raid_io->base_bdev_io_submitted == 0) {
		base_info = &raid_bdev->base_bdev_info[ps_idx];
		base_ch = raid_ch->base_channel[ps_idx];

		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
		if (es_idx == sts_idx) {
			num_blcks = raid5_num_blcks(bdev_io, raid_bdev, sts_idx);
		} else {
			num_blcks = raid_bdev->strip_size;
		}

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
										request->strip_buffs[ps_idx], request->strip_buffs_cnts[ps_idx],
										ofs_blcks, num_blcks,
										raid5_w_r_modify_w_writing_cb,
										request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_r_modify_w_writing, request);
				return;
			}

			base_bdev_io_not_submitted = ((es_idx + raid_bdev->num_base_bdevs) -
						sts_idx) % raid_bdev->num_base_bdevs + 2 - raid_io->base_bdev_io_submitted;
			raid5_w_r_modify_w_writing_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
		++raid_io->base_bdev_io_submitted;
	}
	start_idx = (sts_idx + raid_io->base_bdev_io_submitted - 1) % raid_bdev->num_base_bdevs;

	for (uint8_t idx = start_idx; idx != after_es_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_ch->base_channel[idx];
		
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, idx);
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, idx);

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
											request->strip_buffs[idx], request->strip_buffs_cnts[idx],
											ofs_blcks, num_blcks,
											raid5_w_r_modify_w_writing_cb,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_r_modify_w_writing, request);
				return;
			}

			base_bdev_io_not_submitted = ((es_idx + raid_bdev->num_base_bdevs) -
						sts_idx) % raid_bdev->num_base_bdevs + 2 - raid_io->base_bdev_io_submitted;
			raid5_w_r_modify_w_writing_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		++raid_io->base_bdev_io_submitted;
	}
}

static bool
raid5_w_r_modify_w_reading_complete_part(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		struct raid_bdev 		*raid_bdev = raid_io->raid_bdev;
		struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(raid_io);
		uint64_t block_size_b8 = ((uint64_t)128 * raid_bdev->strip_size_kb) /
					raid_bdev->strip_size;
		uint8_t sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
		uint8_t es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
		uint8_t after_es_idx = raid5_next_idx(es_idx, raid_bdev);
		uint8_t ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
		uint64_t ofs_b8;
		uint64_t num_b8;
		int ret = 0;

		if (raid_io->base_bdev_io_status != SPDK_BDEV_IO_STATUS_SUCCESS) {
			raid5_w_r_modify_w_reading_free_strip_buffs(request);
			raid5_stripe_req_complete(request);
			return true;
		}

		for (uint8_t i = sts_idx; i != after_es_idx; i = raid5_next_idx(i, raid_bdev)) {
			ofs_b8 = (raid5_ofs_blcks(bdev_io, raid_bdev, i) -
					raid5_ofs_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
			num_b8 = raid5_num_blcks(bdev_io, raid_bdev, i) * block_size_b8;
			raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
					request->strip_buffs_cnts[ps_idx], ofs_b8,
					request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
					num_b8);
		}

		ret = raid5_write_r_modify_w_reset_strip_buffs(request);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("RAID5 write request: allocation of buffers is failed\n");
			raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
			raid5_stripe_req_complete(request);
			return true;
		}

		for (uint8_t i = sts_idx; i != after_es_idx; i = raid5_next_idx(i, raid_bdev)) {
			ofs_b8 = (raid5_ofs_blcks(bdev_io, raid_bdev, i) -
					raid5_ofs_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
			num_b8 = raid5_num_blcks(bdev_io, raid_bdev, i) * block_size_b8;
			raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
					request->strip_buffs_cnts[ps_idx], ofs_b8,
					request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
					num_b8);
		}

		raid_io->base_bdev_io_submitted = 0;
		raid_io->base_bdev_io_remaining = (((es_idx + raid_bdev->num_base_bdevs) -
					sts_idx) % raid_bdev->num_base_bdevs) + 2;

		raid5_write_r_modify_w_writing(request);

		return true;
	} else {
		return false;
	}
}

static void
raid5_w_r_modify_w_reading_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5_w_r_modify_w_reading_complete_part(request, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid5_write_r_modify_w_reading(struct raid5_stripe_request *request);

static void
_raid5_write_r_modify_w_reading(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_write_r_modify_w_reading(request);
}

static void
raid5_write_r_modify_w_reading(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			base_bdev_io_not_submitted;
	uint64_t			ofs_blcks;
	uint64_t			num_blcks;
	uint8_t				start_idx;
	int				ret = 0;

	if (raid_io->base_bdev_io_submitted == 0) {
		base_info = &raid_bdev->base_bdev_info[ps_idx];
		base_ch = raid_ch->base_channel[ps_idx];

		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
		if (es_idx == sts_idx) {
			num_blcks = raid5_num_blcks(bdev_io, raid_bdev, sts_idx);
		} else {
			num_blcks = raid_bdev->strip_size;
		}

		ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
										request->strip_buffs[ps_idx], request->strip_buffs_cnts[ps_idx],
										ofs_blcks, num_blcks,
										raid5_w_r_modify_w_reading_cb,
										request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_r_modify_w_reading, request);
				return;
			}

			base_bdev_io_not_submitted = ((es_idx + raid_bdev->num_base_bdevs) -
						sts_idx) % raid_bdev->num_base_bdevs + 2 - raid_io->base_bdev_io_submitted;
			raid5_w_r_modify_w_reading_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
		++raid_io->base_bdev_io_submitted;
	}
	start_idx = (sts_idx + raid_io->base_bdev_io_submitted - 1) % raid_bdev->num_base_bdevs;

	for (uint8_t idx = start_idx; idx != after_es_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_ch->base_channel[idx];
		
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, idx);
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, idx);

		ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
											request->strip_buffs[idx], request->strip_buffs_cnts[idx],
											ofs_blcks, num_blcks,
											raid5_w_r_modify_w_reading_cb,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_r_modify_w_reading, request);
				return;
			}

			base_bdev_io_not_submitted = ((es_idx + raid_bdev->num_base_bdevs) -
						sts_idx) % raid_bdev->num_base_bdevs + 2 - raid_io->base_bdev_io_submitted;
			raid5_w_r_modify_w_reading_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		++raid_io->base_bdev_io_submitted;
	}
}

static bool
raid5_w_br_r_writing_complete_part(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		raid5_w_br_r_writing_free_strip_buffs(request);
		raid5_stripe_req_complete(request);

		return true;
	} else {
		return false;
	}
}

static void
raid5_w_br_r_writing_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5_w_br_r_writing_complete_part(request, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid5_write_broken_req_writing(struct raid5_stripe_request *request);

static void
_raid5_write_broken_req_writing(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_write_broken_req_writing(request);
}

static void
raid5_write_broken_req_writing(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			base_bdev_io_not_submitted;
	uint64_t			ofs_blcks;
	uint64_t			num_blcks;
	uint8_t				start_idx;
	int				ret = 0;

	if (raid_io->base_bdev_io_submitted == 0) {
		base_info = &raid_bdev->base_bdev_info[ps_idx];
		base_ch = raid_ch->base_channel[ps_idx];

		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
		if (es_idx == sts_idx) {
			num_blcks = raid5_num_blcks(bdev_io, raid_bdev, sts_idx);
		} else {
			num_blcks = raid_bdev->strip_size;
		}

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
										request->strip_buffs[ps_idx], request->strip_buffs_cnts[ps_idx],
										ofs_blcks, num_blcks,
										raid5_w_br_r_writing_cb,
										request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_broken_req_writing, request);
				return;
			}

			base_bdev_io_not_submitted = ((es_idx + raid_bdev->num_base_bdevs) -
						sts_idx) % raid_bdev->num_base_bdevs + 1 - raid_io->base_bdev_io_submitted;
			raid5_w_br_r_writing_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}
		++raid_io->base_bdev_io_submitted;
	}
	start_idx = (sts_idx + raid_io->base_bdev_io_submitted - 1) % raid_bdev->num_base_bdevs;
	if (start_idx == request->broken_strip_idx) {
		start_idx = raid5_next_idx(start_idx, raid_bdev);
	}

	for (uint8_t idx = start_idx; idx != after_es_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		if (idx == request->broken_strip_idx) {
			continue;
		}
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_ch->base_channel[idx];
		
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, idx);
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, idx);

		ret = spdk_bdev_writev_blocks(base_info->desc, base_ch,
											request->strip_buffs[idx], request->strip_buffs_cnts[idx],
											ofs_blcks, num_blcks,
											raid5_w_br_r_writing_cb,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_write_broken_req_writing, request);
				return;
			}

			base_bdev_io_not_submitted = ((es_idx + raid_bdev->num_base_bdevs) -
						sts_idx) % raid_bdev->num_base_bdevs + 1 - raid_io->base_bdev_io_submitted;
			raid5_w_br_r_writing_complete_part(request, base_bdev_io_not_submitted,
							SPDK_BDEV_IO_STATUS_FAILED);
			return;
		}

		++raid_io->base_bdev_io_submitted;
	}
}

static bool
raid5_w_br_r_reading_complete_part(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		struct raid_bdev 		*raid_bdev = raid_io->raid_bdev;
		struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(raid_io);
		uint64_t block_size_b8 = ((uint64_t)128 * raid_bdev->strip_size_kb) /
					raid_bdev->strip_size;
		uint8_t sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
		uint8_t after_sts_idx = raid5_next_idx(sts_idx, raid_bdev);
		uint8_t es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
		uint8_t after_es_idx = raid5_next_idx(es_idx, raid_bdev);
		uint8_t ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
		uint8_t after_ps_idx = raid5_next_idx(ps_idx, raid_bdev);
		uint64_t ofs_b8;
		uint64_t num_b8;
		int ret = 0;

		if (raid_io->base_bdev_io_status != SPDK_BDEV_IO_STATUS_SUCCESS) {
			raid5_w_br_r_reading_free_strip_buffs(request);
			raid5_stripe_req_complete(request);
			return true;
		}

		if (sts_idx == es_idx) {
			num_b8 = raid5_num_blcks(bdev_io, raid_bdev, sts_idx) * block_size_b8;
			for (uint8_t i = after_sts_idx; i != ps_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], 0,
						request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
						num_b8);
			}
			for (uint8_t i = after_ps_idx; i != sts_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], 0,
						request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
						num_b8);
			}
		} else if (request->broken_strip_idx != sts_idx && request->broken_strip_idx != es_idx) {
			num_b8 = raid_bdev->strip_size * block_size_b8;
			for (uint8_t i = after_es_idx; i != ps_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], 0,
						request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
						num_b8);
			}
			for (uint8_t i = after_ps_idx; i != sts_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], 0,
						request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
						num_b8);
			}
			num_b8 = (raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx) -
					raid5_ofs_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
			raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
					request->strip_buffs_cnts[ps_idx], 0,
					request->strip_buffs[sts_idx], request->strip_buffs_cnts[sts_idx], 0,
					num_b8);
			num_b8 = (raid_bdev->strip_size - raid5_num_blcks(bdev_io, raid_bdev, es_idx)) *
						block_size_b8;
			ofs_b8 = raid5_num_blcks(bdev_io, raid_bdev, es_idx) * block_size_b8;
			raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
					request->strip_buffs_cnts[ps_idx], ofs_b8,
					request->strip_buffs[es_idx], request->strip_buffs_cnts[es_idx], ofs_b8,
					num_b8);
		} else if (request->broken_strip_idx == sts_idx) {
			ofs_b8 = (raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx) -
						raid5_ofs_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
			num_b8 = raid5_num_blcks(bdev_io, raid_bdev, sts_idx) * block_size_b8;
			for (uint8_t i = ps_idx; i != sts_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[i], request->strip_buffs_cnts[i], ofs_b8,
						num_b8);
			}
			for (uint8_t i = after_es_idx; i != ps_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[i], request->strip_buffs_cnts[i], ofs_b8,
						num_b8);
			}
			ofs_b8 = 0;
			num_b8 = (raid_bdev->strip_size -
					raid5_num_blcks(bdev_io, raid_bdev, sts_idx)) * block_size_b8;
			for (uint8_t i = after_sts_idx; i != es_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[i], request->strip_buffs_cnts[i], ofs_b8,
						num_b8);
			}
			if (raid5_num_blcks(bdev_io, raid_bdev, es_idx) < raid_bdev->strip_size -
					raid5_num_blcks(bdev_io, raid_bdev, sts_idx)) {
				ofs_b8 = 0;
				num_b8 = raid5_num_blcks(bdev_io, raid_bdev, es_idx) * block_size_b8;
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[es_idx], request->strip_buffs_cnts[es_idx], ofs_b8,
						num_b8);

				ofs_b8 = (raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx) -
						raid5_ofs_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
				num_b8 = raid5_num_blcks(bdev_io, raid_bdev, sts_idx) * block_size_b8;
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[es_idx], request->strip_buffs_cnts[es_idx], ofs_b8,
						num_b8);
			} else {
				ofs_b8 = 0;
				num_b8 = (raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx) -
						raid5_ofs_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[es_idx], request->strip_buffs_cnts[es_idx], ofs_b8,
						num_b8);

				ofs_b8 = raid5_num_blcks(bdev_io, raid_bdev, es_idx) * block_size_b8;
				num_b8 = (raid_bdev->strip_size - raid5_num_blcks(bdev_io, raid_bdev, es_idx)) *
						block_size_b8;
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[es_idx], request->strip_buffs_cnts[es_idx], ofs_b8,
						num_b8);
			}
		} else {
			ofs_b8 = 0;
			num_b8 = raid5_num_blcks(bdev_io, raid_bdev, es_idx) * block_size_b8;
			for (uint8_t i = ps_idx; i != sts_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[i], request->strip_buffs_cnts[i], ofs_b8,
						num_b8);
			}
			for (uint8_t i = after_es_idx; i != ps_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[i], request->strip_buffs_cnts[i], ofs_b8,
						num_b8);
			}
			ofs_b8 = raid5_num_blcks(bdev_io, raid_bdev, es_idx) * block_size_b8;
			num_b8 = (raid_bdev->strip_size -
					raid5_num_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
			for (uint8_t i = after_sts_idx; i != es_idx; i = raid5_next_idx(i, raid_bdev)) {
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[i], request->strip_buffs_cnts[i], ofs_b8,
						num_b8);
			}

			if (raid5_num_blcks(bdev_io, raid_bdev, sts_idx) < raid_bdev->strip_size - 
					raid5_num_blcks(bdev_io, raid_bdev, es_idx)) {
				ofs_b8 = (raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx) -
						raid5_ofs_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
				num_b8 = raid5_num_blcks(bdev_io, raid_bdev, sts_idx) * block_size_b8;
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[sts_idx], request->strip_buffs_cnts[sts_idx], ofs_b8,
						num_b8);
				
				ofs_b8 = 0;
				num_b8 = raid5_num_blcks(bdev_io, raid_bdev, es_idx) * block_size_b8;
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[sts_idx], request->strip_buffs_cnts[sts_idx], ofs_b8,
						num_b8);
			} else {
				ofs_b8 = raid5_num_blcks(bdev_io, raid_bdev, es_idx) * block_size_b8;
				num_b8 = (raid_bdev->strip_size - raid5_num_blcks(bdev_io, raid_bdev, es_idx)) *
						block_size_b8;
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[sts_idx], request->strip_buffs_cnts[sts_idx], ofs_b8,
						num_b8);

				ofs_b8 = 0;
				num_b8 = (raid_bdev->strip_size - raid5_num_blcks(bdev_io, raid_bdev, sts_idx))
						* block_size_b8;
				raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
						request->strip_buffs_cnts[ps_idx], ofs_b8,
						request->strip_buffs[sts_idx], request->strip_buffs_cnts[sts_idx], ofs_b8,
						num_b8);
			}
		}

		ret = raid5_write_broken_req_reset_strip_buffs(request);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("RAID5 write request: allocation of buffers is failed\n");
			raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
			raid5_stripe_req_complete(request);
			return true;
		}

		for (uint8_t i = sts_idx; i != after_es_idx; i = raid5_next_idx(i, raid_bdev)) {
			ofs_b8 = (raid5_ofs_blcks(bdev_io, raid_bdev, i) -
					raid5_ofs_blcks(bdev_io, raid_bdev, es_idx)) * block_size_b8;
			num_b8 = raid5_num_blcks(bdev_io, raid_bdev, i) * block_size_b8;
			raid5_xor_iovs_with_iovs(request->strip_buffs[ps_idx],
					request->strip_buffs_cnts[ps_idx], ofs_b8,
					request->strip_buffs[i], request->strip_buffs_cnts[i], 0,
					num_b8);
		}

		raid_io->base_bdev_io_submitted = 0;
		raid_io->base_bdev_io_remaining = (((es_idx + raid_bdev->num_base_bdevs) -
					sts_idx) % raid_bdev->num_base_bdevs) + 1;

		raid5_write_broken_req_writing(request);

		return true;
	} else {
		return false;
	}
}

static void
raid5_w_br_r_reading_cb(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid5_w_br_r_reading_complete_part(request, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static void raid5_write_broken_req_reading(struct raid5_stripe_request *request);

static void
_raid5_write_broken_req_reading(void *cb_arg)
{
	struct raid5_stripe_request *request = cb_arg;
	raid5_write_broken_req_reading(request);
}

static void
raid5_write_broken_req_reading(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_sts_idx = raid5_next_idx(sts_idx, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_es_idx = raid5_next_idx(es_idx, raid_bdev);
	uint64_t			ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	uint64_t			num_strips_to_r;
	uint64_t			base_bdev_io_not_submitted;
	uint64_t			ofs_blcks;
	uint64_t			num_blcks;
	uint8_t				start_idx;
	int				ret = 0;

	if (sts_idx == es_idx) {
		num_strips_to_r = raid_bdev->num_base_bdevs - 2;
		start_idx = (after_sts_idx + raid_io->base_bdev_io_submitted) % raid_bdev->num_base_bdevs;
		if (start_idx == ps_idx) {
			start_idx = raid5_next_idx(start_idx, raid_bdev);
		}
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx);
		num_blcks = raid5_num_blcks(bdev_io, raid_bdev, sts_idx);

		for (uint8_t idx = start_idx; idx != sts_idx; idx = raid5_next_idx(idx, raid_bdev)) {
			if (idx == ps_idx) {
				continue;
			}
			base_info = &raid_bdev->base_bdev_info[idx];
			base_ch = raid_ch->base_channel[idx];

			ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
												request->strip_buffs[idx], request->strip_buffs_cnts[idx],
												ofs_blcks, num_blcks,
												raid5_w_br_r_reading_cb,
												request);

			if (spdk_unlikely(ret != 0)) {
				if (spdk_unlikely(ret == -ENOMEM)) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
								base_ch, _raid5_write_broken_req_reading, request);
					return;
				}

				base_bdev_io_not_submitted = num_strips_to_r - raid_io->base_bdev_io_submitted;
				raid5_w_br_r_reading_complete_part(request, base_bdev_io_not_submitted,
								SPDK_BDEV_IO_STATUS_FAILED);
				return;
			}

			++raid_io->base_bdev_io_submitted;
		}
	} else if (request->broken_strip_idx != sts_idx && request->broken_strip_idx != es_idx) {
		num_strips_to_r = raid_bdev->num_base_bdevs - (((es_idx + raid_bdev->num_base_bdevs) -
					sts_idx) % raid_bdev->num_base_bdevs);
		start_idx = (es_idx + raid_io->base_bdev_io_submitted) % raid_bdev->num_base_bdevs;
		if (start_idx == ps_idx) {
			start_idx = raid5_next_idx(start_idx, raid_bdev);
		}
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
		num_blcks = raid_bdev->strip_size;

		for (uint8_t idx = start_idx; idx != after_sts_idx; idx = raid5_next_idx(idx, raid_bdev)) {
			if (idx == ps_idx) {
				continue;
			}
			base_info = &raid_bdev->base_bdev_info[idx];
			base_ch = raid_ch->base_channel[idx];

			ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
												request->strip_buffs[idx], request->strip_buffs_cnts[idx],
												ofs_blcks, num_blcks,
												raid5_w_br_r_reading_cb,
												request);

			if (spdk_unlikely(ret != 0)) {
				if (spdk_unlikely(ret == -ENOMEM)) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
								base_ch, _raid5_write_broken_req_reading, request);
					return;
				}

				base_bdev_io_not_submitted = num_strips_to_r - raid_io->base_bdev_io_submitted;
				raid5_w_br_r_reading_complete_part(request, base_bdev_io_not_submitted,
								SPDK_BDEV_IO_STATUS_FAILED);
				return;
			}

			++raid_io->base_bdev_io_submitted;
		}
	} else if (request->broken_strip_idx == sts_idx) {
		num_strips_to_r = raid_bdev->num_base_bdevs - 1;
		start_idx = (after_sts_idx + raid_io->base_bdev_io_submitted) % raid_bdev->num_base_bdevs;
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
		num_blcks = raid_bdev->strip_size;

		for (uint8_t idx = start_idx; idx != sts_idx; idx = raid5_next_idx(idx, raid_bdev)) {
			base_info = &raid_bdev->base_bdev_info[idx];
			base_ch = raid_ch->base_channel[idx];

			ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
												request->strip_buffs[idx], request->strip_buffs_cnts[idx],
												ofs_blcks, num_blcks,
												raid5_w_br_r_reading_cb,
												request);

			if (spdk_unlikely(ret != 0)) {
				if (spdk_unlikely(ret == -ENOMEM)) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
								base_ch, _raid5_write_broken_req_reading, request);
					return;
				}

				base_bdev_io_not_submitted = num_strips_to_r - raid_io->base_bdev_io_submitted;
				raid5_w_br_r_reading_complete_part(request, base_bdev_io_not_submitted,
								SPDK_BDEV_IO_STATUS_FAILED);
				return;
			}

			++raid_io->base_bdev_io_submitted;
		}
	} else {
		num_strips_to_r = raid_bdev->num_base_bdevs - 1;
		start_idx = (after_es_idx + raid_io->base_bdev_io_submitted) % raid_bdev->num_base_bdevs;
		ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, es_idx);
		num_blcks = raid_bdev->strip_size;

		for (uint8_t idx = start_idx; idx != es_idx; idx = raid5_next_idx(idx, raid_bdev)) {
			base_info = &raid_bdev->base_bdev_info[idx];
			base_ch = raid_ch->base_channel[idx];

			ret = spdk_bdev_readv_blocks(base_info->desc, base_ch,
												request->strip_buffs[idx], request->strip_buffs_cnts[idx],
												ofs_blcks, num_blcks,
												raid5_w_br_r_reading_cb,
												request);

			if (spdk_unlikely(ret != 0)) {
				if (spdk_unlikely(ret == -ENOMEM)) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
								base_ch, _raid5_write_broken_req_reading, request);
					return;
				}

				base_bdev_io_not_submitted = num_strips_to_r - raid_io->base_bdev_io_submitted;
				raid5_w_br_r_reading_complete_part(request, base_bdev_io_not_submitted,
								SPDK_BDEV_IO_STATUS_FAILED);
				return;
			}

			++raid_io->base_bdev_io_submitted;
		}
	}
}

static void
raid5_submit_write_request(struct raid5_stripe_request *request)
{
	struct raid_bdev_io 		*raid_io = request->raid_io;
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid5_info			*r5_info = raid_bdev->module_private;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct spdk_io_channel		*base_ch;
	uint64_t			sts_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			es_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint8_t				ps_idx = raid5_parity_strip_index(raid_bdev, raid5_stripe_idx(bdev_io, raid_bdev));
	int				ret = 0;

	for (uint8_t idx = 0; idx < raid_bdev->num_base_bdevs; ++idx) {
		base_ch = raid_ch->base_channel[idx];
		
		if (base_ch == NULL) {
			if (request->broken_strip_idx == raid_bdev->num_base_bdevs) {
				request->broken_strip_idx = idx;
			} else {
				SPDK_ERRLOG("RAID5 write request: 2 broken strips\n");
				raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
				raid5_stripe_req_complete(request);
				assert(false);
				return;
			}
		}
	}
	
	if (request->broken_strip_idx == raid_bdev->num_base_bdevs &&
				r5_info->write_type == DEFAULT) {
		// default

		ret = raid5_write_default_set_strip_buffs(request);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("RAID5 write request: allocation of buffers is failed\n");
			raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
			raid5_stripe_req_complete(request);
			return;
		}

		raid_io->base_bdev_io_submitted = 0;
		raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs - (((es_idx + raid_bdev->num_base_bdevs) -
				sts_idx) % raid_bdev->num_base_bdevs) - 2;
		if (sts_idx != es_idx) {
			raid_io->base_bdev_io_remaining +=2;
		}
		raid5_write_default_reading(request);
	} else if (request->broken_strip_idx == ps_idx) {
		// broken parity strip

		ret = raid5_write_broken_parity_set_strip_buffs(request);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("RAID5 write request: allocation of buffers is failed\n");
			raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
			raid5_stripe_req_complete(request);
			return;
		}

		raid_io->base_bdev_io_submitted = 0;
		raid_io->base_bdev_io_remaining = ((es_idx + raid_bdev->num_base_bdevs) -
				sts_idx) % raid_bdev->num_base_bdevs + 1;
		raid5_write_broken_parity_strip(request);
	} else if (request->broken_strip_idx == raid_bdev->num_base_bdevs ||
					!raid5_is_req_strip(sts_idx, es_idx,request->broken_strip_idx)) {
		// read-modify-write

		ret = raid5_write_r_modify_w_set_strip_buffs(request);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("RAID5 write request: allocation of buffers is failed\n");
			raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
			raid5_stripe_req_complete(request);
			return;
		}

		raid_io->base_bdev_io_submitted = 0;
		raid_io->base_bdev_io_remaining = ((es_idx + raid_bdev->num_base_bdevs) -
				sts_idx) % raid_bdev->num_base_bdevs + 2;
		raid5_write_r_modify_w_reading(request);
	} else {
		// broken req strip

		ret = raid5_write_broken_req_set_strip_buffs(request);
		if (spdk_unlikely(ret != 0)) {
			SPDK_ERRLOG("RAID5 write request: allocation of buffers is failed\n");
			raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
			raid5_stripe_req_complete(request);
			return;
		}

		raid_io->base_bdev_io_submitted = 0;
		if (sts_idx == es_idx) {
			raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs - 2;
		} else if (request->broken_strip_idx != sts_idx && request->broken_strip_idx != es_idx) {
			raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs - (((es_idx + raid_bdev->num_base_bdevs) -
							sts_idx) % raid_bdev->num_base_bdevs);
		} else {
			raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs - 1;
		}

		raid5_write_broken_req_reading(request);
	}
}

static void
raid5_submit_rw_request(struct raid_bdev_io *raid_io)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct raid_bdev			*raid_bdev = raid_io->raid_bdev;
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid5_info			*r5_info = raid_bdev->module_private;
	struct raid5_stripe_request	*request;

	if (!raid5_check_io_boundaries(raid_io)) {
		SPDK_ERRLOG("RAID5: I/O spans stripe boundaries!\n");
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
		assert(false);
		return;
	}

	raid5_check_raid_ch(raid_ch);

	if (bdev_io->type == SPDK_BDEV_IO_TYPE_WRITE && r5_info->write_type == UNDEFINED) {
		SPDK_ERRLOG("RAID5: write type is undefinied\n");
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	request = raid5_get_stripe_request(raid_io);
	if (request == NULL) {
		SPDK_ERRLOG("RAID5: allocation of stripe request is failed\n");
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	switch (bdev_io->type) {
	case SPDK_BDEV_IO_TYPE_READ:
		raid5_submit_read_request(request);
		break;
	case SPDK_BDEV_IO_TYPE_WRITE:
		raid5_submit_write_request(request);
		break;
	default:
		SPDK_ERRLOG("RAID5: Invalid request type");
		raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
		raid5_stripe_req_complete(request);
		assert(false);
	}
}

static bool
raid5_wz_req_complete_part_final(struct raid_bdev_io *raid_io, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	assert(raid_io->base_bdev_io_remaining >= completed);
	raid_io->base_bdev_io_remaining -= completed;

	if (status != SPDK_BDEV_IO_STATUS_SUCCESS) {
		raid_io->base_bdev_io_status = status;
	}

	if (raid_io->base_bdev_io_remaining == 0) {
		struct raid5_info *r5_info = raid_io->raid_bdev->module_private;

		if (raid_io->base_bdev_io_status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			r5_info->write_type = READ_MODIFY_WRITE;
			SPDK_NOTICELOG("raid5 write_type: READ_MODIFY_WRITE\n");
		} else {
			r5_info->write_type = DEFAULT;
			SPDK_NOTICELOG("raid5 write_type: DEFAULT\n");
		}

		raid_bdev_destroy_cb(raid_io->raid_bdev, raid_io->raid_ch);
		spdk_dma_free(raid_io->raid_ch);
		spdk_dma_free(raid_io);
		return true;
	} else {
		return false;
	}
}

static void
raid5_wz_req_complete_part(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid_bdev_io *raid_io = cb_arg;
	
	spdk_bdev_free_io(bdev_io);

	raid5_wz_req_complete_part_final(raid_io, 1, success ?
					SPDK_BDEV_IO_STATUS_SUCCESS :
					SPDK_BDEV_IO_STATUS_FAILED);
}

static int
raid5_submit_write_zeroes_request(struct raid_bdev_io *raid_io);

static void
_raid5_submit_write_zeroes_request(void *_raid_io)
{
	struct raid_bdev_io *raid_io = _raid_io;

	raid5_submit_write_zeroes_request(raid_io);
}

static int
raid5_submit_write_zeroes_request(struct raid_bdev_io *raid_io) {
	struct raid_bdev *raid_bdev = raid_io->raid_bdev;
	struct raid_base_bdev_info *base_info;
	struct spdk_io_channel *base_ch;
	uint64_t num_blocks = raid_bdev->bdev.blockcnt / (raid_bdev->num_base_bdevs - 1);
	uint64_t base_bdev_io_not_submitted;
	int ret = 0;

	if (raid_io->base_bdev_io_submitted == 0) {
		raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs;
	}

	for (uint8_t idx = raid_io->base_bdev_io_submitted; idx < raid_bdev->num_base_bdevs; ++idx) {
		base_info = &raid_bdev->base_bdev_info[idx];
		base_ch = raid_io->raid_ch->base_channel[idx];

		if (base_ch == NULL) {
			raid_io->base_bdev_io_submitted++;
			raid5_wz_req_complete_part_final(raid_io, 1, SPDK_BDEV_IO_STATUS_SUCCESS);
			continue;
		}
		
		ret = spdk_bdev_write_zeroes_blocks(base_info->desc, base_ch,
							0, num_blocks,
							raid5_wz_req_complete_part, raid_io);
		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid_bdev_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_submit_write_zeroes_request);
				return 0;
			}

			base_bdev_io_not_submitted = raid_bdev->num_base_bdevs -
							raid_io->base_bdev_io_submitted;
			raid5_wz_req_complete_part_final(raid_io,
							base_bdev_io_not_submitted,
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
raid5_set_write_type(struct raid_bdev *raid_bdev)
{
	struct spdk_bdev_desc *desc;
	struct spdk_bdev *base_bdev;
	struct raid_bdev_io *raid_io;
	struct raid5_info *r5_info = raid_bdev->module_private;
	int ret;

	r5_info->write_type = UNDEFINED;

	for (uint8_t idx = 0; idx < raid_bdev->num_base_bdevs; ++idx) {
		desc = raid_bdev->base_bdev_info[idx].desc;
		if (desc != NULL) {
			base_bdev = spdk_bdev_desc_get_bdev(desc);
			if (!base_bdev->fn_table->io_type_supported(base_bdev->ctxt,
								SPDK_BDEV_IO_TYPE_WRITE_ZEROES)) {
				r5_info->write_type = DEFAULT;
				return;
			}
		}
	}
	
	raid_io = spdk_dma_malloc(sizeof(struct raid_bdev_io), 0, NULL);
	if (raid_io == NULL) {
		r5_info->write_type = DEFAULT;
		return;
	}
	
	raid_io->raid_bdev = raid_bdev;
	raid_io->base_bdev_io_remaining = 0;
	raid_io->base_bdev_io_submitted = 0;
	raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_SUCCESS;
	raid_io->raid_ch = spdk_dma_malloc(sizeof(struct raid_bdev_io_channel), 0, NULL);
	if (raid_io->raid_ch == NULL) {
		spdk_dma_free(raid_io);
		r5_info->write_type = DEFAULT;
		return;
	}

	ret = raid_bdev_create_cb(raid_bdev, raid_io->raid_ch);
	if (ret != 0) {
		spdk_dma_free(raid_io->raid_ch);
		spdk_dma_free(raid_io);
		r5_info->write_type = DEFAULT;
		return;
	}

	ret = raid5_submit_write_zeroes_request(raid_io);
	if (spdk_unlikely(ret != 0)) {
		raid_bdev_destroy_cb(raid_bdev, raid_io->raid_ch);
		spdk_dma_free(raid_io->raid_ch);
		spdk_dma_free(raid_io);
		r5_info->write_type = DEFAULT;
		return;
	}
}

static uint64_t
raid5_calculate_blockcnt(struct raid_bdev *raid_bdev)
{
	uint64_t min_blockcnt = UINT64_MAX;
	struct raid_base_bdev_info *base_info;
	uint64_t total_stripes;
	uint64_t stripe_blockcnt;

	RAID_FOR_EACH_BASE_BDEV(raid_bdev, base_info) {
		min_blockcnt = spdk_min(min_blockcnt, spdk_bdev_desc_get_bdev(base_info->desc)->blockcnt);
	}

	total_stripes = min_blockcnt / raid_bdev->strip_size;
	stripe_blockcnt = raid_bdev->strip_size * (raid_bdev->num_base_bdevs - 1);

	SPDK_DEBUGLOG(bdev_raid5, "min blockcount %" PRIu64 ",  numbasedev %u, strip size shift %u\n",
				min_blockcnt, raid_bdev->num_base_bdevs, raid_bdev->strip_size_shift);

	return total_stripes * stripe_blockcnt;
}

static int
raid5_start(struct raid_bdev *raid_bdev)
{
	struct raid5_info *r5_info;
	uint32_t logic_stripe_size = raid_bdev->strip_size * (raid_bdev->num_base_bdevs - 1);

	raid_bdev->bdev.blockcnt = raid5_calculate_blockcnt(raid_bdev);
	raid_bdev->bdev.optimal_io_boundary = logic_stripe_size;
	raid_bdev->bdev.split_on_optimal_io_boundary = true;
	raid_bdev->min_base_bdevs_operational = raid_bdev->num_base_bdevs - 1;

	r5_info = spdk_dma_malloc((sizeof(struct raid5_info)), 0, NULL);
	assert(r5_info != NULL);
	raid_bdev->module_private = r5_info;

	raid5_set_write_type(raid_bdev);

	return 0;
}

static void
raid5_resize(struct raid_bdev *raid_bdev)
{
	uint64_t blockcnt;
	int rc;

	blockcnt = raid5_calculate_blockcnt(raid_bdev);

	if (blockcnt == raid_bdev->bdev.blockcnt) {
		return;
	}

	SPDK_NOTICELOG("raid5 '%s': min blockcount was changed from %" PRIu64 " to %" PRIu64 "\n",
					raid_bdev->bdev.name,
					raid_bdev->bdev.blockcnt,
					blockcnt);

	rc = spdk_bdev_notify_blockcnt_change(&raid_bdev->bdev, blockcnt);
	if (rc != 0) {
		SPDK_ERRLOG("Failed to notify blockcount change\n");
	}
}

static bool
raid5_stop(struct raid_bdev *raid_bdev) {
	spdk_dma_free(raid_bdev->module_private);
	return true;
}

static struct raid_bdev_module g_raid5_module = {
	.level = RAID5,
	.base_bdevs_min = 3,
	.memory_domains_supported = false,
	.start = raid5_start,
	.stop = raid5_stop,
	.submit_rw_request = raid5_submit_rw_request,
	.resize = raid5_resize
};
RAID_MODULE_REGISTER(&g_raid5_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_raid5)
