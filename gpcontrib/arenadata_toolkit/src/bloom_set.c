/*
 * Set of blooms. Main entry point to find a bloom and work with it.
 * Used to track create, extend, truncate events.
 */

#include "bloom_set.h"

#include <libpq/md5.h>

#define BLOOM_ENTRY_GET(set, i) (void *)(set->bloom_entries + i * FULL_BLOOM_ENTRY_SIZE(set->bloom_size));

static bloom_entry_t * bloom_entry_init(const uint32_t bloom_size, void *mem)
{
	bloom_entry_t *bloom_entry = mem;

	bloom_entry->lock = LWLockAssign();
	bloom_entry->dbid = InvalidOid;
	(void)bloom_init(bloom_size, &bloom_entry->bloom);

	return bloom_entry;
}

bloom_set_t *
bloom_set_init(const uint32_t bloom_count, const uint32_t bloom_size, void *mem)
{
	bloom_set_t *bloom_set = mem;

	bloom_set->bloom_count = bloom_count;
	bloom_set->bloom_size = bloom_size;
	bloom_set->lock = LWLockAssign();

	for (uint32_t i = 0; i < bloom_count; i++)
	{
		void	   *bloom_entry_mem = BLOOM_ENTRY_GET(bloom_set, i);

		(void)bloom_entry_init(bloom_size, bloom_entry_mem);
	}

	return bloom_set;
}

/* simple linear search, probably should be reworked (depends on target dbs count) */
static bloom_entry_t * find_bloom_entry(bloom_set_t * bloom_set, Oid dbid, bool *found)
{
	bloom_entry_t *bloom_entry;
	int			i;

	*found = false;

	for (i = 0; i < bloom_set->bloom_count; i++)
	{
		bloom_entry = BLOOM_ENTRY_GET(bloom_set, i);
		if (bloom_entry->dbid == dbid || bloom_entry->dbid == InvalidOid)
			break;
	}

	if (i == bloom_set->bloom_count)
		return NULL;

	if (bloom_entry->dbid != InvalidOid)
		*found = true;

	return bloom_entry;
}

/* bind not used filter to given dbid */
bool
bloom_set_bind(bloom_set_t * bloom_set, Oid dbid)
{
	bloom_entry_t *bloom_entry;
	bool		found;

	LWLockAcquire(bloom_set->lock, LW_SHARED);
	bloom_entry = find_bloom_entry(bloom_set, dbid, &found);
	LWLockRelease(bloom_set->lock);

	if (found)
		return true;
	else if (!bloom_entry)
	{
		return false;
	}

	LWLockAcquire(bloom_set->lock, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(bloom_set, dbid, &found);
	if (bloom_entry && !found)
		LWLockAcquire(bloom_entry->lock, LW_EXCLUSIVE);
	LWLockRelease(bloom_set->lock);

	if (!bloom_entry)
	{
		elog(WARNING, "Our bloom filter was stolen :(");
		return false;
	}

	if (!found)
	{
		bloom_entry->dbid = dbid;
		LWLockRelease(bloom_entry->lock);
		elog(DEBUG1, "Bloom binded %d", dbid);
	}


	return true;
}

bool
bloom_set_trigger_bits(bloom_set_t * bloom_set, Oid dbid, bool on)
{
	bloom_entry_t *bloom_entry;
	bool		found;

	LWLockAcquire(bloom_set->lock, LW_SHARED);
	{
		bloom_entry = find_bloom_entry(bloom_set, dbid, &found);
	}
	LWLockRelease(bloom_set->lock);

	if (!found)
	{
		elog(LOG, "[arenadata toolkit] tracking_initial_snapshot Bloom filter not found");
		return false;
	}

	LWLockAcquire(bloom_entry->lock, LW_EXCLUSIVE);
	{
		if (on)
			bloom_set_all(&bloom_entry->bloom);
		else
			bloom_clear(&bloom_entry->bloom);
	}
	LWLockRelease(bloom_entry->lock);
	return true;
}

/* unbind used filter by given dbid */
void
bloom_set_unbind(bloom_set_t * bloom_set, Oid dbid)
{
	bloom_entry_t *bloom_entry;
	bool		found;

	LWLockAcquire(bloom_set->lock, LW_SHARED);
	bloom_entry = find_bloom_entry(bloom_set, dbid, &found);
	LWLockRelease(bloom_set->lock);

	if (!found)
		return;

	LWLockAcquire(bloom_set->lock, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(bloom_set, dbid, &found);
	if (bloom_entry && found)
		LWLockAcquire(bloom_entry->lock, LW_EXCLUSIVE);
	LWLockRelease(bloom_set->lock);

	if (found)
	{
		bloom_entry->dbid = InvalidOid;
		bloom_clear(&bloom_entry->bloom);
		LWLockRelease(bloom_entry->lock);
		elog(DEBUG1, "Bloom unbinded %d", dbid);
	}
}

uint64_t
bloom_set_calc_hash(const void *buf, size_t len)
{
	struct wide_hash
	{
		uint64_t	i1;
		uint64_t	i2;
	};
	struct wide_hash w_hash;
	bool		hash_res = pg_md5_binary(buf, len, &w_hash);

	Assert(hash_res);

	return w_hash.i1 ^ w_hash.i2;
}

/* find bloom by dbid, set bit based on relNode hash */
void
bloom_set_set(bloom_set_t * bloom_s, Oid dbid, Oid relNode)
{
	bloom_entry_t *bloom_entry;
	bool		found;
	uint64_t	hash;

	LWLockAcquire(bloom_s->lock, LW_SHARED);
	bloom_entry = find_bloom_entry(bloom_s, dbid, &found);
	if (found)
		LWLockAcquire(bloom_entry->lock, LW_EXCLUSIVE);
	LWLockRelease(bloom_s->lock);

	if (!found)
		return;

	hash = bloom_set_calc_hash(&relNode, sizeof(relNode));
	bloom_set(&bloom_entry->bloom, hash);
	LWLockRelease(bloom_entry->lock);

	elog(DEBUG1, "Bloom set %d %d", dbid, relNode);
}

/* find bloom by dbid, copy all bytes to new filter, clear old (but keep it) */
bloom_t *
bloom_set_move(bloom_set_t * bloom_set, Oid dbid, void *mem)
{
	bloom_entry_t *bloom_entry;
	bool		found;
	bloom_t    *copy;

	LWLockAcquire(bloom_set->lock, LW_SHARED);
	bloom_entry = find_bloom_entry(bloom_set, dbid, &found);
	if (found)
		LWLockAcquire(bloom_entry->lock, LW_EXCLUSIVE);
	LWLockRelease(bloom_set->lock);

	/* no bloom for the database */
	if (!found)
		return NULL;

	copy = bloom_copy(&bloom_entry->bloom, mem);
	bloom_clear(&bloom_entry->bloom);
	LWLockRelease(bloom_entry->lock);

	elog(DEBUG1, "Bloom moved %d", dbid);

	return copy;
}

/* find bloom by dbid, merge bytes from another bloom to it */
bool
bloom_set_merge(bloom_set_t * bloom_set, Oid dbid, bloom_t * m_bloom)
{
	bloom_entry_t *bloom_entry;
	bool		found;

	if (!m_bloom)
		return false;

	LWLockAcquire(bloom_set->lock, LW_SHARED);
	bloom_entry = find_bloom_entry(bloom_set, dbid, &found);
	if (found)
		LWLockAcquire(bloom_entry->lock, LW_EXCLUSIVE);
	LWLockRelease(bloom_set->lock);

	if (!found)
		return false;

	bloom_merge(&bloom_entry->bloom, m_bloom);
	LWLockRelease(bloom_entry->lock);

	elog(DEBUG1, "Bloom merged %d", dbid);

	return true;
}

bool
bloom_set_is_all_bits_triggered(bloom_set_t * bloom_set, Oid dbid)
{
	bloom_entry_t *bloom_entry;
	bool		found;
	bool		is_triggered;

	LWLockAcquire(bloom_set->lock, LW_SHARED);
	bloom_entry = find_bloom_entry(bloom_set, dbid, &found);
	LWLockRelease(bloom_set->lock);

	if (!found)
	{
		return false;
	}

	LWLockAcquire(bloom_entry->lock, LW_SHARED);
	is_triggered = bloom_entry->bloom.is_set_all;
	LWLockRelease(bloom_entry->lock);

	return is_triggered;
}
