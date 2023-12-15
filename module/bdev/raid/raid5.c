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

struct raid5_io_buffer {
	struct raid_bdev_io *raid_io;

	struct iovec *buffer;
};

struct raid5_write_request_buffer {
	struct raid5_io_buffer *wr_xor_buff;
	
	struct iovec *buffer;
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

	buffer = calloc(1, sizeof(*buffer));
	if (buffer == NULL) {
		return NULL;
	}

	buffer->iov_len = iovlen;
	buffer->iov_base = calloc(buffer->iov_len, sizeof(char));
	if (buffer->iov_base == NULL) {
		free(buffer);
		return NULL;
	}

	return buffer;
}

static inline void
raid5_free_buffer(struct iovec *buffer)
{
	free(buffer->iov_base);
	free(buffer);
}

static inline struct raid5_io_buffer *
raid5_get_io_buffer(struct raid_bdev_io *raid_io, size_t data_len)
{
	struct raid5_io_buffer *io_buffer;

	io_buffer = calloc(1, sizeof(struct raid5_io_buffer));
	if (io_buffer == NULL) {
		return NULL;
	}

	io_buffer->buffer = raid5_get_buffer(data_len);
	if (io_buffer->buffer == NULL) {
		free(io_buffer);
		return NULL;
	}

	io_buffer->raid_io = raid_io;
	return io_buffer;
}

static inline void
raid5_free_io_buffer(struct raid5_io_buffer *io_buffer)
{
	raid5_free_buffer(io_buffer->buffer);
	free(io_buffer);
}

static inline struct raid5_write_request_buffer *
raid5_get_write_request_buffer(struct raid5_io_buffer *wr_xor_buff, size_t data_len)
{
	struct raid5_write_request_buffer *wr_buffer;

	wr_buffer = calloc(1, sizeof(struct raid5_write_request_buffer));
	if (wr_buffer == NULL) {
		return NULL;
	}

	wr_buffer->buffer = raid5_get_buffer(data_len);
	if (wr_buffer->buffer == NULL) {
		free(wr_buffer);
		return NULL;
	}

	wr_buffer->wr_xor_buff = wr_xor_buff;
	return wr_buffer;
}

static inline void
raid5_free_write_request_buffer(struct raid5_write_request_buffer *wr_buffer)
{
	raid5_free_buffer(wr_buffer->buffer);
	free(wr_buffer);
}

static inline void
raid5_xor_buffers(struct iovec *xor_res, struct iovec *buffer)
{
	uint64_t *xb8 = xor_res->iov_base;
	uint64_t *b8 = buffer->iov_base;
	size_t len8 = xor_res->iov_len / 8;

	for (size_t i=0; i < len8; ++i) {
		xb8[i] ^= b8[i];
	}
}

static inline void
raid5_xor_iovs_with_buffer(struct iovec *iovs, int iovcnt, struct iovec *buffer)
{
	uint64_t *xb8;
	uint64_t *b8 = buffer->iov_base;
	size_t b8i = 0;
	size_t len8;

	for (int iovidx = 0; iovidx < iovcnt; ++iovidx) {
		xb8 = iovs[iovidx].iov_base;
		len8 = iovs[iovidx].iov_len / 8;
		for (size_t i = 0; i < len8; ++i, ++b8i) {
			xb8[i] ^= b8[b8i];
		}
	}
}

static inline void
raid5_xor_buffer_with_iovs(struct iovec *buffer, struct iovec *iovs, int iovcnt)
{
	uint64_t *xb8 = buffer->iov_base;
	uint64_t *b8;
	size_t xb8i = 0;
	size_t len8;

	for (int iovidx = 0; iovidx < iovcnt; ++iovidx) {
		b8 = iovs[iovidx].iov_base;
		len8 = iovs[iovidx].iov_len / 8;
		for (size_t i = 0; i < len8; ++i, ++xb8i) {
			xb8[xb8i] ^= b8[i];
		}
	}
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

static void
raid5_bdev_io_completion(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid_bdev_io *raid_io = cb_arg;

	spdk_bdev_free_io(bdev_io);

	raid_bdev_io_complete(raid_io, success ?
				   SPDK_BDEV_IO_STATUS_SUCCESS :
				   SPDK_BDEV_IO_STATUS_FAILED);
}

static void
raid5_read_request_complete_part(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid5_io_buffer *io_buffer = cb_arg;
	struct spdk_bdev_io		*rbdev_io = spdk_bdev_io_from_ctx(io_buffer->raid_io);

	spdk_bdev_free_io(bdev_io);

	assert(io_buffer->raid_io->base_bdev_io_remaining > 0);
	io_buffer->raid_io->base_bdev_io_remaining--;

	if (!success) {
		io_buffer->raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
	} else {
		raid5_xor_iovs_with_buffer(rbdev_io->u.bdev.iovs, rbdev_io->u.bdev.iovcnt,
				io_buffer->buffer);
	}

	if (io_buffer->raid_io->base_bdev_io_remaining == 0) {
		raid_bdev_io_complete(io_buffer->raid_io,
				io_buffer->raid_io->base_bdev_io_status);
	}

	raid5_free_io_buffer(io_buffer);
}

static void raid5_submit_write_request_writing(struct raid5_io_buffer *io_buffer);

static void
raid5_write_request_reading_complete_part (struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid5_write_request_buffer *wr_buffer = cb_arg;
	struct spdk_bdev_io		*rbdev_io = spdk_bdev_io_from_ctx(wr_buffer->wr_xor_buff->raid_io);

	spdk_bdev_free_io(bdev_io);

	assert(wr_buffer->wr_xor_buff->raid_io->base_bdev_io_remaining > 0);
	wr_buffer->wr_xor_buff->raid_io->base_bdev_io_remaining--;

	if (!success) {
		wr_buffer->wr_xor_buff->raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
	} else {
		raid5_xor_buffers(wr_buffer->wr_xor_buff->buffer, wr_buffer->buffer);
	}

	if (wr_buffer->wr_xor_buff->raid_io->base_bdev_io_remaining == 0) {
		if (wr_buffer->wr_xor_buff->raid_io->base_bdev_io_status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			raid5_xor_buffer_with_iovs(wr_buffer->wr_xor_buff->buffer,
					rbdev_io->u.bdev.iovs, rbdev_io->u.bdev.iovcnt);
			wr_buffer->wr_xor_buff->raid_io->base_bdev_io_submitted = 1;
			wr_buffer->wr_xor_buff->raid_io->base_bdev_io_remaining = 1;
			raid5_submit_write_request_writing(wr_buffer->wr_xor_buff);
		} else {
			raid_bdev_io_complete(wr_buffer->wr_xor_buff->raid_io,
					wr_buffer->wr_xor_buff->raid_io->base_bdev_io_status);
			raid5_free_io_buffer(wr_buffer->wr_xor_buff);
		}
	}

	raid5_free_write_request_buffer(wr_buffer);
}

static void
raid5_write_request_reading_with_writing_req_strip_complete_part(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid5_write_request_buffer *wr_buffer = cb_arg;
	struct spdk_bdev_io		*rbdev_io = spdk_bdev_io_from_ctx(wr_buffer->wr_xor_buff->raid_io);
	
	spdk_bdev_free_io(bdev_io);

	assert(wr_buffer->wr_xor_buff->raid_io->base_bdev_io_remaining > 0);
	wr_buffer->wr_xor_buff->raid_io->base_bdev_io_remaining--;

	if (!success) {
		wr_buffer->wr_xor_buff->raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
	} else {
		raid5_xor_buffers(wr_buffer->wr_xor_buff->buffer, wr_buffer->buffer);
	}
	
	if (wr_buffer->wr_xor_buff->raid_io->base_bdev_io_remaining == 0) {
		if (wr_buffer->wr_xor_buff->raid_io->base_bdev_io_status == SPDK_BDEV_IO_STATUS_SUCCESS) {
			raid5_xor_buffer_with_iovs(wr_buffer->wr_xor_buff->buffer,
					rbdev_io->u.bdev.iovs, rbdev_io->u.bdev.iovcnt);
			wr_buffer->wr_xor_buff->raid_io->base_bdev_io_submitted = 0;
			wr_buffer->wr_xor_buff->raid_io->base_bdev_io_remaining = 2;
			raid5_submit_write_request_writing(wr_buffer->wr_xor_buff);
		} else {
			raid_bdev_io_complete(wr_buffer->wr_xor_buff->raid_io,
					wr_buffer->wr_xor_buff->raid_io->base_bdev_io_status);
			raid5_free_io_buffer(wr_buffer->wr_xor_buff);
		}
	}

	raid5_free_write_request_buffer(wr_buffer);
}

static void
raid5_write_request_writing_complete_part(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct raid5_io_buffer *io_buffer = cb_arg;
	
	spdk_bdev_free_io(bdev_io);

	assert(io_buffer->raid_io->base_bdev_io_remaining > 0);
	io_buffer->raid_io->base_bdev_io_remaining--;

	if (!success) {
		io_buffer->raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
	}

	if (io_buffer->raid_io->base_bdev_io_remaining == 0) {
		raid_bdev_io_complete(io_buffer->raid_io,
				io_buffer->raid_io->base_bdev_io_status);
		raid5_free_io_buffer(io_buffer);	
	}
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

	SPDK_ERRLOG("raid5_xor_iovs_with_iovs\n");

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
			for (uint64_t i = ofs8; i < (iovs[idx].iov_len / 8); ++i) {
				xb8[i - ofs8 + xofs8] ^= b8[i];
			}
			num_b8 -= iovs[idx].iov_len / 8 - ofs8;
			++idx;
			ofs8 = 0;
		} else if (xor_iovs[xor_idx].iov_len / 8 - xofs8 <
				iovs[idx].iov_len / 8 - ofs8) {
			for (uint64_t i = xofs8; i < (xor_iovs[xor_idx].iov_len / 8); ++i) {
				xb8[i] ^= b8[i - xofs8 + ofs8];
			}
			num_b8 -= xor_iovs[xor_idx].iov_len / 8 - xofs8;
			++xor_idx;
			xofs8 = 0;
		} else {
			for (uint64_t i = ofs8; i < (iovs[idx].iov_len / 8); ++i) {
				xb8[i - ofs8 + xofs8] ^= b8[i];
			}
			num_b8 -= iovs[idx].iov_len / 8 - ofs8;
			++idx;
			ofs8 = 0;
			++xor_idx;
			xofs8 = 0;
		}
	}	
}

static struct raid5_stripe_request *
raid5_get_stripe_request(struct raid_bdev_io *raid_io)
{
	struct raid5_stripe_request *request;

	request = calloc(1, sizeof(struct raid5_stripe_request));
	if (request == NULL) {
		return NULL;
	}

	request->raid_io = raid_io;
	request->strip_buffs_cnt = raid_io->raid_bdev->num_base_bdevs;
	request->broken_strip_idx = raid_io->raid_bdev->num_base_bdevs;
	request->strip_buffs = calloc(request->strip_buffs_cnt, sizeof(struct iovec *));
	if (request->strip_buffs == NULL) {
		free(request);
		return NULL;
	}

	request->strip_buffs_cnts = calloc(request->strip_buffs_cnt, sizeof(int));
	if (request->strip_buffs_cnts == NULL) {
		free(request->strip_buffs);
		free(request);
		return NULL;
	}

	return request;
}

static void
raid5_free_stripe_request(struct raid5_stripe_request *request) {
	free(request->strip_buffs_cnts);
	free(request->strip_buffs);
	free(request);
}

static int
raid5_allocate_strips_buffs_until(struct raid5_stripe_request *request,
				uint8_t start_idx, uint8_t until_idx, uint64_t num_blcks)
{
	struct raid_bdev *raid_bdev = request->raid_io->raid_bdev;
	uint64_t block_size_b = ((uint64_t)1024 * raid_bdev->strip_size_kb) / raid_bdev->strip_size;

	SPDK_ERRLOG("raid5_allocate_strips_buffs_until\n");

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

	SPDK_ERRLOG("raid5_free_strips_buffs_until\n");

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

	SPDK_ERRLOG("raid5_set_req_strips_iovs_until\n");

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
		request->strip_buffs[idx] = calloc(request->strip_buffs_cnts[idx], sizeof(struct iovec));
		if (request->strip_buffs[idx] == NULL) {
			for (uint8_t i = start_idx; i != idx; i = raid5_next_idx(i, raid_bdev)) {
				free(request->strip_buffs[i]);
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

	SPDK_ERRLOG("raid5_free_req_strips_iovs_until\n");

	for (uint8_t idx = start_idx; idx != until_idx; idx = raid5_next_idx(idx, raid_bdev)) {
		free(request->strip_buffs[idx]);
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

	SPDK_ERRLOG("raid5_set_all_req_strips_iovs\n");
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

	SPDK_ERRLOG("raid5_set_all_strip_buffs\n");

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
	request->strip_buffs[sts_idx] = calloc(request->strip_buffs_cnts[sts_idx], sizeof(struct iovec));
	if (request->strip_buffs[sts_idx] == NULL) {
		raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
		request->strip_buffs_cnts[sts_idx] = 0;
		return -ENOMEM;
	}
	
	len = blocks * block_size_b;
	
	iov_base_b8 = bdev_io->u.bdev.iovs[iov_idx].iov_base;
	request->strip_buffs[sts_idx][sts_idx_ofs].iov_base =
			&iov_base_b8[(bdev_io->u.bdev.iovs[iov_idx].iov_len - remaining_len) / 8];

	SPDK_ERRLOG("iov_base_b8: %llu\n", iov_base_b8);
	SPDK_ERRLOG("idx: %lu\n", (bdev_io->u.bdev.iovs[iov_idx].iov_len - remaining_len) / 8);
	SPDK_ERRLOG("iov_base: %llu\n", request->strip_buffs[sts_idx][sts_idx_ofs].iov_base);
	SPDK_ERRLOG("remaining len: %lu\n", remaining_len);
	SPDK_ERRLOG("iov_idx: %d", iov_idx);

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
		request->strip_buffs[sts_idx][0].iov_base = calloc(request->strip_buffs[sts_idx][0].iov_len,
						sizeof(char));
		if (request->strip_buffs[sts_idx][0].iov_base == NULL) {
			raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
			free(request->strip_buffs[sts_idx]);
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
			free(request->strip_buffs[sts_idx][0].iov_base);
		}
		free(request->strip_buffs[sts_idx]);
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
	request->strip_buffs[es_idx] = calloc(request->strip_buffs_cnts[es_idx], sizeof(struct iovec));
	if (request->strip_buffs[es_idx] == NULL) {
		raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
		if (sts_idx_ofs == 1) {
			free(request->strip_buffs[sts_idx][0].iov_base);
		}
		free(request->strip_buffs[sts_idx]);
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
						calloc(request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx]
						- 1].iov_len,
						sizeof(char));
		if (request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx] - 1].iov_base
				== NULL) {
			raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);
			if (sts_idx_ofs == 1) {
				free(request->strip_buffs[sts_idx][0].iov_base);
			}
			free(request->strip_buffs[sts_idx]);
			request->strip_buffs[sts_idx] = NULL;
			request->strip_buffs_cnts[sts_idx] = 0;
			raid5_free_req_strips_iovs_until(request,
							raid5_next_idx(sts_idx, raid_bdev), es_idx);
			free(request->strip_buffs[es_idx]);
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

	SPDK_ERRLOG("raid5_free_all_strip_buffs\n");

	raid5_free_strips_buffs_until(request, after_es_idx, sts_idx);

	if (ofs_blcks != raid5_ofs_blcks(bdev_io, raid_bdev, sts_idx)) {
		free(request->strip_buffs[sts_idx][0].iov_base);
	}
	free(request->strip_buffs[sts_idx]);
	request->strip_buffs[sts_idx] = NULL;
	request->strip_buffs_cnts[sts_idx] = 0;
	if (sts_idx == es_idx) {
		return;
	}

	raid5_free_req_strips_iovs_until(request,
					raid5_next_idx(sts_idx, raid_bdev), es_idx);

	if (ofs_blcks + num_blcks > raid5_ofs_blcks(bdev_io, raid_bdev, es_idx) +
			raid5_num_blcks(bdev_io, raid_bdev, es_idx)) {
		free(request->strip_buffs[es_idx][request->strip_buffs_cnts[es_idx]
				- 1].iov_base);
	}
	free(request->strip_buffs[es_idx]);
	request->strip_buffs[es_idx] = NULL;
	request->strip_buffs_cnts[es_idx] = 0;
}

static int
raid5_read_req_strips_set_strip_buffs(struct raid5_stripe_request *request)
{
	SPDK_ERRLOG("raid5_read_req_strips_set_strip_buffs\n");

	return raid5_set_all_req_strips_iovs(request);
}

static void
raid5_read_req_strips_free_strip_buffs(struct raid5_stripe_request *request)
{
	SPDK_ERRLOG("raid5_read_req_strips_free_strip_buffs\n");

	raid5_free_all_req_strips_iovs(request);
}

static int
raid5_read_exc_req_strip_set_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, request->broken_strip_idx);
	uint64_t			num_blcks = raid5_num_blcks(bdev_io, raid_bdev, request->broken_strip_idx);

	SPDK_ERRLOG("raid5_read_exc_req_strip_set_strip_buffs\n");

	return raid5_set_all_strip_buffs(request, ofs_blcks, num_blcks);
}

static void
raid5_read_exc_req_strip_free_strip_buffs(struct raid5_stripe_request *request)
{
	struct spdk_bdev_io			*bdev_io = spdk_bdev_io_from_ctx(request->raid_io);
	struct raid_bdev			*raid_bdev = request->raid_io->raid_bdev;
	uint64_t			ofs_blcks = raid5_ofs_blcks(bdev_io, raid_bdev, request->broken_strip_idx);
	uint64_t			num_blcks = raid5_num_blcks(bdev_io, raid_bdev, request->broken_strip_idx);

	SPDK_ERRLOG("raid5_read_exc_req_strip_free_strip_buffs\n");

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

	SPDK_ERRLOG("raid5_write_default_set_strip_buffs\n");

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

	SPDK_ERRLOG("raid5_write_default_free_strip_buffs\n");

	if (sts_idx != es_idx) {
		return raid5_free_all_strip_buffs(request, es_ofs_blcks, raid_bdev->strip_size);
	} else {
		return raid5_free_all_strip_buffs(request, es_ofs_blcks, es_num_blcks);
	}
}

static int
raid5_write_broken_parity_set_strip_buffs(struct raid5_stripe_request *request)
{
	SPDK_ERRLOG("raid5_write_broken_parity_set_strip_buffs\n");

	return raid5_set_all_req_strips_iovs(request);
}

static void
raid5_write_broken_parity_free_strip_buffs(struct raid5_stripe_request *request)
{
	SPDK_ERRLOG("raid5_write_broken_parity_free_strip_buffs\n");
	
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
raid5_read_complete_part_final(struct raid5_stripe_request *request, uint64_t completed,
				enum spdk_bdev_io_status status)
{
	struct raid_bdev_io *raid_io = request->raid_io;

	SPDK_ERRLOG("raid5_read_complete_part_final\n");

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
			assert(0);
		} else {
			raid5_read_req_strips_free_strip_buffs(request);
		}

		raid5_stripe_req_complete(request);
		return true;
	} else {
		return false;
	}
}

static void raid5_read_complete_part(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	struct raid5_stripe_request *request = cb_arg;

	SPDK_ERRLOG("raid5_read_complete_part\n");

	spdk_bdev_free_io(bdev_io);

	raid5_read_complete_part_final(request, 1, success ?
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

	SPDK_ERRLOG("raid5_read_req_strips\n");

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
											raid5_read_complete_part,
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
			raid5_read_complete_part_final(request, base_bdev_io_not_submitted,
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

	SPDK_ERRLOG("raid5_read_except_one_req_strip\n");

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
											raid5_read_complete_part,
											request);

		if (spdk_unlikely(ret != 0)) {
			if (spdk_unlikely(ret == -ENOMEM)) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_read_except_one_req_strip, request);
				return;
			}

			base_bdev_io_not_submitted = raid_bdev->num_base_bdevs - 1 -
							raid_io->base_bdev_io_submitted;
			raid5_read_complete_part_final(request, base_bdev_io_not_submitted,
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

	SPDK_ERRLOG("raid5_submit_read_request");

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

static void raid5_submit_rw_request(struct raid_bdev_io *raid_io);

static void
_raid5_submit_rw_request(void *_raid_io)
{
	struct raid_bdev_io *raid_io = _raid_io;

	raid5_submit_rw_request(raid_io);
}

static void raid5_submit_write_request_reading(struct raid5_io_buffer *wr_xor_buff);

static void
_raid5_submit_write_request_reading(void *_wr_xor_buff)
{
	struct raid5_io_buffer *wr_xor_buff = _wr_xor_buff;

	raid5_submit_write_request_reading(wr_xor_buff);
}

static void
raid5_submit_write_request_reading(struct raid5_io_buffer *wr_xor_buff)
{
	struct raid_bdev_io *raid_io = wr_xor_buff->raid_io;
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct spdk_bdev_ext_io_opts	io_opts = {};
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_bdev		*raid_bdev = raid_io->raid_bdev;
	uint64_t			block_size_b = (raid_bdev->strip_size_kb / raid_bdev->strip_size) * (uint64_t)1024;
	uint8_t				broken_bdev_idx = raid_bdev->num_base_bdevs;
	uint64_t			stripe_index;
	uint64_t			parity_strip_idx;
	uint64_t			req_bdev_idx;
	uint32_t			offset_in_strip;
	uint64_t			offset_blocks;
	uint64_t			num_blocks;
	int				ret = 0;
	uint64_t			start_strip_idx;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;
	struct raid5_write_request_buffer *wr_buffer;

	start_strip_idx = bdev_io->u.bdev.offset_blocks >> raid_bdev->strip_size_shift;

	io_opts.size = sizeof(io_opts);
	io_opts.memory_domain = bdev_io->u.bdev.memory_domain;
	io_opts.memory_domain_ctx = bdev_io->u.bdev.memory_domain_ctx;
	io_opts.metadata = bdev_io->u.bdev.md_buf;

	stripe_index = start_strip_idx / (raid_bdev->num_base_bdevs - 1);
	parity_strip_idx = raid5_parity_strip_index(raid_bdev, stripe_index);
	offset_in_strip = bdev_io->u.bdev.offset_blocks % (raid_bdev->strip_size);

	req_bdev_idx = start_strip_idx % (raid_bdev->num_base_bdevs - 1);
	if (req_bdev_idx >= parity_strip_idx) {
		++req_bdev_idx;
	}
	offset_blocks = (stripe_index << raid_bdev->strip_size_shift) + offset_in_strip;
	num_blocks = bdev_io->u.bdev.num_blocks;
	
	// calculating of broken strip idx
	for (uint8_t idx = 0; idx < raid_bdev->num_base_bdevs; ++idx) {
		if (raid_ch->base_channel[idx] == NULL) {
			if (broken_bdev_idx == raid_bdev->num_base_bdevs) {
				broken_bdev_idx = idx;
			} else {
				SPDK_ERRLOG("2 broken strips\n");
				assert(false);
				raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
				if (raid_io->base_bdev_io_submitted == 0) {
					raid_bdev_io_complete(raid_io, raid_io->base_bdev_io_status);
				}
				return;
			}
		}
	}

	if (broken_bdev_idx != req_bdev_idx && broken_bdev_idx != raid_bdev->num_base_bdevs) {
		// case: broken strip isn't request strip or parity strip

		if (raid_io->base_bdev_io_submitted == 0) {
			raid_io->base_bdev_io_remaining = 2;
		}
		
		switch (raid_io->base_bdev_io_submitted) {
			case 0:
				base_info = &raid_bdev->base_bdev_info[parity_strip_idx];
				base_ch = raid_ch->base_channel[parity_strip_idx];
				
				wr_buffer = raid5_get_write_request_buffer(wr_xor_buff, num_blocks * block_size_b);
				if (wr_buffer == NULL) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_submit_write_request_reading, wr_xor_buff);
					return;
				}

				ret = spdk_bdev_readv_blocks_ext(base_info->desc, base_ch,
							wr_buffer->buffer, 1,
							offset_blocks, num_blocks, raid5_write_request_reading_with_writing_req_strip_complete_part,
							wr_buffer, &io_opts);
				
				if (ret != 0) {
					raid5_free_write_request_buffer(wr_buffer);
					if (ret == -ENOMEM) {
						raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
								base_ch, _raid5_submit_write_request_reading, wr_xor_buff);
					} else {
						SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
						assert(false);
						raid5_free_io_buffer(wr_xor_buff);
						raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
					}
					return;
				}
				raid_io->base_bdev_io_submitted++;
			case 1:
				base_info = &raid_bdev->base_bdev_info[req_bdev_idx];
				base_ch = raid_ch->base_channel[req_bdev_idx];
				
				wr_buffer = raid5_get_write_request_buffer(wr_xor_buff, num_blocks * block_size_b);
				if (wr_buffer == NULL) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_submit_write_request_reading, wr_xor_buff);
					return;
				}

				ret = spdk_bdev_readv_blocks_ext(base_info->desc, base_ch,
							wr_buffer->buffer, 1,
							offset_blocks, num_blocks, raid5_write_request_reading_with_writing_req_strip_complete_part,
							wr_buffer, &io_opts);
							
				if (ret != 0) {
					raid5_free_write_request_buffer(wr_buffer);
					if (ret == -ENOMEM) {
						raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
								base_ch, _raid5_submit_write_request_reading, wr_xor_buff);
					} else {
						SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
						assert(false);
						raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
						raid_io->base_bdev_io_remaining = raid_io->base_bdev_io_remaining + raid_io->base_bdev_io_submitted - 2;
						if (raid_io->base_bdev_io_remaining == 0) {
							raid5_free_io_buffer(wr_xor_buff);
							raid_bdev_io_complete(raid_io, raid_io->base_bdev_io_status);
						}
					}
					return;
				}
				raid_io->base_bdev_io_submitted++;
		}
	} else {
		// cases with reading stripe

		uint8_t start_idx;
		spdk_bdev_io_completion_cb cb;
		
		if (broken_bdev_idx == req_bdev_idx) {
			// case: broken request strip
			cb = raid5_write_request_reading_complete_part;
		} else {
			// case: without broken strip
			cb = raid5_write_request_reading_with_writing_req_strip_complete_part;
		}

		if (raid_io->base_bdev_io_submitted == 0) {
			raid_io->base_bdev_io_remaining = raid_bdev->num_base_bdevs - 2;
		}

		start_idx = raid_io->base_bdev_io_submitted;
		if (req_bdev_idx <= start_idx || parity_strip_idx <= start_idx) {
			start_idx++;
			if (req_bdev_idx <= start_idx && parity_strip_idx <= start_idx)  {
				start_idx++;
			}
		}

		for (uint8_t idx = start_idx; idx < raid_bdev->num_base_bdevs; ++idx) {
			if (idx == req_bdev_idx || idx == parity_strip_idx) {
				continue;
			}

			base_info = &raid_bdev->base_bdev_info[idx];
			base_ch = raid_ch->base_channel[idx];

			wr_buffer = raid5_get_write_request_buffer(wr_xor_buff, num_blocks * block_size_b);
			if (wr_buffer == NULL) {
				raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
						base_ch, _raid5_submit_write_request_reading, wr_xor_buff);
				return;
			}

			ret = spdk_bdev_readv_blocks_ext(base_info->desc, base_ch,
						wr_buffer->buffer, 1,
						offset_blocks, num_blocks, cb,
						wr_buffer, &io_opts);

			if (ret != 0) {
				raid5_free_write_request_buffer(wr_buffer);
				if (ret == -ENOMEM) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_submit_write_request_reading, wr_xor_buff);
				} else {
					SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
					assert(false);
					raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
					raid_io->base_bdev_io_remaining = raid_io->base_bdev_io_remaining + raid_io->base_bdev_io_submitted -
															(raid_bdev->num_base_bdevs - 2);
					if (raid_io->base_bdev_io_remaining == 0) {
						raid5_free_io_buffer(wr_xor_buff);
						raid_bdev_io_complete(raid_io, raid_io->base_bdev_io_status);
					}
				}
				return;
			}
			raid_io->base_bdev_io_submitted++;
		}
	}
}

static void
_raid5_submit_write_request_writing(void *_io_buffer)
{
	struct raid5_io_buffer *io_buffer = _io_buffer;

	raid5_submit_write_request_writing(io_buffer);
}

static void
raid5_submit_write_request_writing(struct raid5_io_buffer *io_buffer)
{
	struct raid_bdev_io *raid_io = io_buffer->raid_io;
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct spdk_bdev_ext_io_opts	io_opts = {};
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_bdev		*raid_bdev = raid_io->raid_bdev;
	uint64_t			stripe_index;
	uint64_t			parity_strip_idx;
	uint64_t			req_bdev_idx;
	uint32_t			offset_in_strip;
	uint64_t			offset_blocks;
	uint64_t			num_blocks;
	int				ret = 0;
	uint64_t			start_strip_idx;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	start_strip_idx = bdev_io->u.bdev.offset_blocks >> raid_bdev->strip_size_shift;

	io_opts.size = sizeof(io_opts);
	io_opts.memory_domain = bdev_io->u.bdev.memory_domain;
	io_opts.memory_domain_ctx = bdev_io->u.bdev.memory_domain_ctx;
	io_opts.metadata = bdev_io->u.bdev.md_buf;

	stripe_index = start_strip_idx / (raid_bdev->num_base_bdevs - 1);
	parity_strip_idx = raid5_parity_strip_index(raid_bdev, stripe_index);
	offset_in_strip = bdev_io->u.bdev.offset_blocks % (raid_bdev->strip_size);

	req_bdev_idx = start_strip_idx % (raid_bdev->num_base_bdevs - 1);
	if (req_bdev_idx >= parity_strip_idx) {
		++req_bdev_idx;
	}
	offset_blocks = (stripe_index << raid_bdev->strip_size_shift) + offset_in_strip;
	num_blocks = bdev_io->u.bdev.num_blocks;

	switch (raid_io->base_bdev_io_submitted) {
		case 0:
			// writing request strip

			base_info = &raid_bdev->base_bdev_info[req_bdev_idx];
			base_ch = raid_ch->base_channel[req_bdev_idx];

			ret = spdk_bdev_writev_blocks_ext(base_info->desc, base_ch,
						bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						offset_blocks, num_blocks, raid5_write_request_writing_complete_part,
						io_buffer, &io_opts);
						
			if (ret != 0) {
				if (ret == -ENOMEM) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_submit_write_request_writing, io_buffer);
				} else {
					SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
					assert(false);
					raid5_free_io_buffer(io_buffer);
					raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
				}
				return;
			}

			raid_io->base_bdev_io_submitted++;
		case 1:
			// writing parity strip
			
			base_info = &raid_bdev->base_bdev_info[parity_strip_idx];
			base_ch = raid_ch->base_channel[parity_strip_idx];

			ret = spdk_bdev_writev_blocks_ext(base_info->desc, base_ch,
						io_buffer->buffer, 1,
						offset_blocks, num_blocks, raid5_write_request_writing_complete_part,
						io_buffer, &io_opts);
			
			if (ret != 0) {
				if (ret == -ENOMEM) {
					raid5_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
							base_ch, _raid5_submit_write_request_writing, io_buffer);
				} else {
					SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
					assert(false);
					raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_FAILED;
					raid_io->base_bdev_io_remaining = raid_io->base_bdev_io_remaining + raid_io->base_bdev_io_submitted - 2;
					if (raid_io->base_bdev_io_remaining == 0) {
						raid5_free_io_buffer(io_buffer);
						raid_bdev_io_complete(raid_io, raid_io->base_bdev_io_status);
					}
				}
				return;
			}

			raid_io->base_bdev_io_submitted++;
	}
}

static void
raid5_submit_write_request(struct raid_bdev_io *raid_io)
{
	struct spdk_bdev_io		*bdev_io = spdk_bdev_io_from_ctx(raid_io);
	struct spdk_bdev_ext_io_opts	io_opts = {};
	struct raid_bdev_io_channel	*raid_ch = raid_io->raid_ch;
	struct raid_bdev		*raid_bdev = raid_io->raid_bdev;
	uint64_t			block_size_b = (raid_bdev->strip_size_kb / raid_bdev->strip_size) * (uint64_t)1024;
	uint8_t				broken_bdev_idx = raid_bdev->num_base_bdevs;
	uint64_t			stripe_index;
	uint64_t			parity_strip_idx;
	uint64_t			req_bdev_idx;
	uint32_t			offset_in_strip;
	uint64_t			offset_blocks;
	uint64_t			num_blocks;
	int				ret = 0;
	uint64_t			start_strip_idx;
	uint64_t			end_strip_idx;
	struct raid_base_bdev_info	*base_info;
	struct spdk_io_channel		*base_ch;

	start_strip_idx = bdev_io->u.bdev.offset_blocks >> raid_bdev->strip_size_shift;
	end_strip_idx = (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1) >>
		    raid_bdev->strip_size_shift;
	if (start_strip_idx != end_strip_idx) {
		SPDK_ERRLOG("I/O spans strip boundary!\n");
		assert(false);
		raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
		return;
	}

	assert(raid_ch != NULL);
	assert(raid_ch->base_channel);

	io_opts.size = sizeof(io_opts);
	io_opts.memory_domain = bdev_io->u.bdev.memory_domain;
	io_opts.memory_domain_ctx = bdev_io->u.bdev.memory_domain_ctx;
	io_opts.metadata = bdev_io->u.bdev.md_buf;

	stripe_index = start_strip_idx / (raid_bdev->num_base_bdevs - 1);
	parity_strip_idx = raid5_parity_strip_index(raid_bdev, stripe_index);
	offset_in_strip = bdev_io->u.bdev.offset_blocks % (raid_bdev->strip_size);

	req_bdev_idx = start_strip_idx % (raid_bdev->num_base_bdevs - 1);
	if (req_bdev_idx >= parity_strip_idx) {
		++req_bdev_idx;
	}
	offset_blocks = (stripe_index << raid_bdev->strip_size_shift) + offset_in_strip;
	num_blocks = bdev_io->u.bdev.num_blocks;

	// calculating of broken strip idx
	for (uint8_t idx = 0; idx < raid_bdev->num_base_bdevs; ++idx) {
		if (raid_ch->base_channel[idx] == NULL) {
			if (broken_bdev_idx == raid_bdev->num_base_bdevs) {
				broken_bdev_idx = idx;
			} else {
				SPDK_ERRLOG("2 broken strips\n");
				assert(false);
				raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
				return;
			}
		}
	}

	if (broken_bdev_idx == parity_strip_idx) {
		// case: broken parity strip

		base_info = &raid_bdev->base_bdev_info[req_bdev_idx];
		base_ch = raid_ch->base_channel[req_bdev_idx];

		ret = spdk_bdev_writev_blocks_ext(base_info->desc, base_ch,
						  bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt,
						  offset_blocks, num_blocks, raid5_bdev_io_completion,
						  raid_io, &io_opts);
		
		if (ret == -ENOMEM) {
			raid_bdev_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
					base_ch, _raid5_submit_rw_request);
		} else if (ret != 0) {
			SPDK_ERRLOG("bdev io submit error not due to ENOMEM, it should not happen\n");
			assert(false);
			raid_bdev_io_complete(raid_io, SPDK_BDEV_IO_STATUS_FAILED);
		}
	} else {
		// cases with parity recalculating

		struct raid5_io_buffer *io_buffer;

		base_info = &raid_bdev->base_bdev_info[parity_strip_idx];
		base_ch = raid_ch->base_channel[parity_strip_idx];

		io_buffer = raid5_get_io_buffer(raid_io, num_blocks * block_size_b);
		if (io_buffer == NULL) {
			raid_bdev_queue_io_wait(raid_io, spdk_bdev_desc_get_bdev(base_info->desc),
					base_ch, _raid5_submit_rw_request);
			return;
		}
		
		raid5_submit_write_request_reading(io_buffer);
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

	// debug logs

	SPDK_ERRLOG("iovcnt: %d\n", bdev_io->u.bdev.iovcnt);

	for (int i = 0; i < bdev_io->u.bdev.iovcnt; ++i) {
		SPDK_ERRLOG("iovec #%d, addr:%llu, len: %lu\n",
				i,
				bdev_io->u.bdev.iovs[i].iov_base,
				bdev_io->u.bdev.iovs[i].iov_len);
	}

	uint64_t			start_strip;
	uint64_t			end_strip;
	uint64_t			ststrip_idx = raid5_start_strip_idx(bdev_io, raid_bdev);
	uint64_t			estrip_idx = raid5_end_strip_idx(bdev_io, raid_bdev);
	uint64_t			after_estrip_idx = raid5_next_idx(estrip_idx, raid_bdev);

	start_strip = bdev_io->u.bdev.offset_blocks >> raid_bdev->strip_size_shift;
	end_strip = (bdev_io->u.bdev.offset_blocks + bdev_io->u.bdev.num_blocks - 1) >>
			raid_bdev->strip_size_shift;

	SPDK_ERRLOG("start_strip: %lu\nend_strip: %lu\nststrip_idx: %lu\nestrip_idx: %lu\nafter_estrip_idx: %lu\n",
			start_strip,
			end_strip,
			ststrip_idx,
			estrip_idx,
			after_estrip_idx);

	// end debug logs

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
		raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_SUCCESS;
		raid5_stripe_req_complete(request);
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
		free(raid_io->raid_ch);
		free(raid_io);
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
	
	raid_io = calloc(1, sizeof(struct raid_bdev_io));
	if (raid_io == NULL) {
		r5_info->write_type = DEFAULT;
		return;
	}
	
	raid_io->raid_bdev = raid_bdev;
	raid_io->base_bdev_io_remaining = 0;
	raid_io->base_bdev_io_submitted = 0;
	raid_io->base_bdev_io_status = SPDK_BDEV_IO_STATUS_SUCCESS;
	raid_io->raid_ch = calloc(1, sizeof(struct raid_bdev_io_channel));
	if (raid_io->raid_ch == NULL) {
		free(raid_io);
		r5_info->write_type = DEFAULT;
		return;
	}

	ret = raid_bdev_create_cb(raid_bdev, raid_io->raid_ch);
	if (ret != 0) {
		free(raid_io->raid_ch);
		free(raid_io);
		r5_info->write_type = DEFAULT;
		return;
	}

	ret = raid5_submit_write_zeroes_request(raid_io);
	if (spdk_unlikely(ret != 0)) {
		raid_bdev_destroy_cb(raid_bdev, raid_io->raid_ch);
		free(raid_io->raid_ch);
		free(raid_io);
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

	r5_info = calloc(1, (sizeof(struct raid5_info)));
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

static struct raid_bdev_module g_raid5_module = {
	.level = RAID5,
	.base_bdevs_min = 3,
	.memory_domains_supported = false,
	.start = raid5_start,
	.submit_rw_request = raid5_submit_rw_request,
	.resize = raid5_resize
};
RAID_MODULE_REGISTER(&g_raid5_module)

SPDK_LOG_REGISTER_COMPONENT(bdev_raid5)
