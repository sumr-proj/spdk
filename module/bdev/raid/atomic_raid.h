/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2018 Intel Corporation.
 *   All rights reserved.
 */

#ifndef SPDK_ATOMIC_RAID_INTERNAL_H
#define SPDK_ATOMIC_RAID_INTERNAL_H

#include "spdk/util.h"

//typedef int raid_atomic; //реализовать можно позже, но пока не вижу смысла

typedef uint64_t raid_atomic64;

#define atomic_read(ptr)       (*(__typeof__(*ptr) *volatile) (ptr))
#define atomic_set(ptr, i)     ((*(__typeof__(*ptr) *volatile) (ptr)) = (i))
#define atomic_inc(ptr)        ((void) __sync_fetch_and_add(ptr, 1))
#define atomic_dec(ptr)        ((void) __sync_fetch_and_add(ptr, -1))
#define atomic_add(ptr, n)     ((void) __sync_fetch_and_add(ptr, n))
#define atomic_sub(ptr, n)     ((void) __sync_fetch_and_sub(ptr, n))

#define atomic_cmpxchg_bool        __sync_bool_compare_and_swap
#define atomic_cmpxchg_val       __sync_val_compare_and_swap



static inline uint64_t
raid_atomic64_read(const raid_atomic64 *a)
{
	return atomic_read(a);
}

static inline void
raid_atomic64_set(raid_atomic64 *a, uint64_t i)
{
	atomic_set(a, i);
}

static inline void
raid_atomic64_add(uint64_t i, raid_atomic64 *a)
{
	atomic_add(a, i);
}

static inline void
raid_atomic64_sub(uint64_t i, raid_atomic64 *a)
{
	atomic_sub(a, i);
}

static inline void
raid_atomic64_inc(raid_atomic64 *a)
{
	atomic_inc(a);
}

static inline void
raid_atomic64_dec(raid_atomic64 *a)
{
	atomic_dec(a);
}

static inline uint64_t
raid_atomic64_add_return(uint64_t i, raid_atomic64 *a)
{
	return __sync_add_and_fetch(a, i);
}

static inline uint64_t
raid_atomic64_sub_return(uint64_t i, raid_atomic64 *a)
{
	return __sync_sub_and_fetch(a, i);
}

static inline uint64_t
raid_atomic64_inc_return(raid_atomic64 *a)
{
	return raid_atomic64_add_return(1, a);
}

static inline uint64_t
raid_atomic64_dec_return(raid_atomic64 *a)
{
	return raid_atomic64_sub_return(1, a);
}

static inline bool
raid_atomic64_cmpxchg_bool(raid_atomic64 *a, uint64_t old_val, uint64_t new_val)
{
	return atomic_cmpxchg_bool(a, old_val, new_val);
}

static inline uint64_t
raid_atomic64_cmpxchg_val(raid_atomic64 *a, uint64_t old_val, uint64_t new_val)
{
	return atomic_cmpxchg_val(a, old_val, new_val);
}

static inline void 
raid_atomic64_set_bit(raid_atomic64 *atomic_ptr, uint64_t shift_size)
{
	uint64_t old_val;
	uint64_t new_val;
	do
	{
		old_val = raid_atomic64_read(atomic_ptr);
		new_val = old_val;
		SPDK_SET_BIT(&new_val, shift_size);
	} while (raid_atomic64_cmpxchg_bool(atomic_ptr, old_val, new_val));
}

static inline void 
raid_atomic64_remove_bit(raid_atomic64 *atomic_ptr, uint64_t shift_size)
{
	uint64_t old_val;
	uint64_t new_val;
	do
	{
		old_val = raid_atomic64_read(atomic_ptr);
		new_val = old_val;
		SPDK_REMOVE_BIT(&new_val, shift_size);
	} while (raid_atomic64_cmpxchg_bool(atomic_ptr, old_val, new_val));
}

#endif /* SPDK_ATOMIC_RAID_INTERNAL_H */