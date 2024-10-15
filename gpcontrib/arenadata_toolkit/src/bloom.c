/*
 * Simple bloom filter without using postgres primitives.
 */
#include "bloom.h"

#include <string.h>

bloom_t *
bloom_init(const uint32_t bloom_size, void *mem)
{
	bloom_t    *bloom = mem;

	bloom->size = bloom_size;
	bloom_clear(bloom);

	return bloom;
}

static uint32_t
calc_idx(bloom_t * bloom, uint64_t hash, uint8_t *bit_idx)
{
	uint64_t	bloom_bit_idx = hash % (8 * bloom->size);

	*bit_idx = bloom_bit_idx % 8;

	return bloom_bit_idx / 8;
}

int
bloom_isset(bloom_t * bloom, uint64_t hash)
{
	uint8_t		bit_idx;
	uint32_t	byte_idx = calc_idx(bloom, hash, &bit_idx);

	return bloom->map[byte_idx] & (1 << bit_idx);
}

void
bloom_set(bloom_t * bloom, uint64_t hash)
{
	uint8_t		bit_idx;
	uint32_t	byte_idx = calc_idx(bloom, hash, &bit_idx);

	bloom->map[byte_idx] |= (1 << bit_idx);
}

void
bloom_set_all(bloom_t * bloom)
{
	memset(bloom->map, 0xFF, bloom->size);
	bloom->is_set_all = 1;
}

void
bloom_clear(bloom_t * bloom)
{
	memset(bloom->map, 0, bloom->size);
	bloom->is_set_all = 0;
}

void
bloom_merge(bloom_t * dst, bloom_t * src)
{
	for (uint32_t i = 0; i < dst->size; i++)
		dst->map[i] |= src->map[i];
	if (src->is_set_all)
		dst->is_set_all = src->is_set_all;
}

bloom_t *
bloom_copy(bloom_t * bloom, void *mem)
{
	bloom_t    *copy;

	copy = bloom_init(bloom->size, mem);
	memcpy(copy->map, bloom->map, bloom->size);
	copy->is_set_all = bloom->is_set_all;

	return copy;
}
