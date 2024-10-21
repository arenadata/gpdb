/*
 * Set of blooms. Main entry point to find a bloom and work with it.
 * Used to track create, extend, truncate events.
 */

#include "bloom_set.h"
#include "tf_shmem.h"

#include <libpq/md5.h>

#define BLOOM_ENTRY_GET(set, i) (void *)(set->bloom_entries + i * FULL_BLOOM_ENTRY_SIZE(set->bloom_size));

static void
bloom_entry_init(const uint32_t bloom_size, bloom_entry_t *bloom_entry)
{
	bloom_entry->dbid = InvalidOid;
	bloom_init(bloom_size, &bloom_entry->bloom);
}

void
bloom_set_init(const uint32_t bloom_count, const uint32_t bloom_size, bloom_set_t *bloom_set)
{
	bloom_set->bloom_count = bloom_count;
	bloom_set->bloom_size = bloom_size;

	for (uint32_t i = 0; i < bloom_count; i++)
	{
		bloom_entry_t	   *bloom_entry = BLOOM_ENTRY_GET(bloom_set, i);

		bloom_entry_init(bloom_size, bloom_entry);
	}
}

/*
 * Finds the entry in bloom_set by given dbid.
 * That's a simple linear search, probably should be reworked (depends on target dbs count).
 */
static bloom_entry_t *
find_bloom_entry(bloom_set_t * bloom_set, Oid dbid)
{
	bloom_entry_t *bloom_entry;
	int i = 0;

	for (i = 0; i < bloom_set->bloom_count; i++)
	{
		bloom_entry = BLOOM_ENTRY_GET(bloom_set, i);
		if (bloom_entry->dbid == dbid)
			break;
	}

	if (i == bloom_set->bloom_count)
		return NULL;

	return bloom_entry;
}

/* Bind available filter to given dbid */
bool
bloom_set_bind(bloom_set_t * bloom_set, Oid dbid)
{
	bloom_entry_t *bloom_entry;

	LWLockAcquire(bloom_set_lock, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(bloom_set, dbid);
	if (bloom_entry)
	{
		LWLockRelease(bloom_set_lock);
		return true;
	}
	bloom_entry = find_bloom_entry(bloom_set, InvalidOid);
	if (bloom_entry == NULL)
	{
		LWLockRelease(bloom_set_lock);
		return false;
	}
	bloom_entry->dbid = dbid;
	LWLockBindEntry(dbid);
	LWLockRelease(bloom_set_lock);

	return true;
}

bool
bloom_set_trigger_bits(bloom_set_t * bloom_set, Oid dbid, bool on)
{
	bloom_entry_t *bloom_entry;
	LWLock *entry_lock;

	LWLockAcquire(bloom_set_lock, LW_SHARED);
	entry_lock = LWLockAcquireEntry(dbid, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(bloom_set, dbid);
	if (bloom_entry)
	{
		if (on)
			bloom_set_all(&bloom_entry->bloom);
		else
			bloom_clear(&bloom_entry->bloom);
		if (entry_lock)
			LWLockRelease(entry_lock);
		LWLockRelease(bloom_set_lock);
		return true;
	}
	if (entry_lock)
		LWLockRelease(entry_lock);
	LWLockRelease(bloom_set_lock);

	if (bloom_entry == NULL)
		elog(LOG, "[arenadata toolkit] tracking_initial_snapshot Bloom filter not found");

	return false;
}

/* Unbind used filter by given dbid */
void
bloom_set_unbind(bloom_set_t * bloom_set, Oid dbid)
{
	bloom_entry_t *bloom_entry;

	LWLockAcquire(bloom_set_lock, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(bloom_set, dbid);
	if (bloom_entry == NULL)
	{
		LWLockRelease(bloom_set_lock);
		return;
	}
	bloom_entry->dbid = InvalidOid;
	bloom_clear(&bloom_entry->bloom);
	LWLockUnbindEntry(dbid);
	LWLockRelease(bloom_set_lock);
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

/* Find bloom by dbid, set bit based on relNode hash */
void
bloom_set_set(bloom_set_t * bloom_s, Oid dbid, Oid relNode)
{
	bloom_entry_t *bloom_entry;
	uint64_t	hash;
	LWLock *entry_lock;

	LWLockAcquire(bloom_set_lock, LW_SHARED);
	entry_lock = LWLockAcquireEntry(dbid, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(bloom_s, dbid);
	if (bloom_entry)
	{
		hash = bloom_set_calc_hash(&relNode, sizeof(relNode));
		bloom_set(&bloom_entry->bloom, hash);
	}
	if (entry_lock)
		LWLockRelease(entry_lock);
	LWLockRelease(bloom_set_lock);
}

/* Find bloom by dbid, copy all bytes to new filter, clear old (but keep it) */
bool
bloom_set_move(bloom_set_t * bloom_set, Oid dbid, bloom_t *dest)
{
	bloom_entry_t *bloom_entry;
	LWLock *entry_lock;

	LWLockAcquire(bloom_set_lock, LW_SHARED);
	entry_lock = LWLockAcquireEntry(dbid, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(bloom_set, dbid);
	if (bloom_entry)
	{
		bloom_copy(&bloom_entry->bloom, dest);
		bloom_clear(&bloom_entry->bloom);
		if (entry_lock)
			LWLockRelease(entry_lock);
		LWLockRelease(bloom_set_lock);
		return true;
	}
	if (entry_lock)
		LWLockRelease(entry_lock);
	LWLockRelease(bloom_set_lock);

	return false;
}

/* Find bloom by dbid, merge bytes from another bloom to it */
bool
bloom_set_merge(bloom_set_t * bloom_set, Oid dbid, bloom_t * m_bloom)
{
	bloom_entry_t *bloom_entry;
	LWLock *entry_lock;

	if (!m_bloom || !bloom_set)
		return false;

	LWLockAcquire(bloom_set_lock, LW_SHARED);
	entry_lock = LWLockAcquireEntry(dbid, LW_EXCLUSIVE);
	bloom_entry = find_bloom_entry(bloom_set, dbid);
	if (bloom_entry)
	{
		bloom_merge(&bloom_entry->bloom, m_bloom);
		if (entry_lock)
			LWLockRelease(entry_lock);
		LWLockRelease(bloom_set_lock);
		return true;
	}
	if (entry_lock)
			LWLockRelease(entry_lock);
	LWLockRelease(bloom_set_lock);

	return false;
}

bool
bloom_set_is_all_bits_triggered(bloom_set_t * bloom_set, Oid dbid)
{
	bloom_entry_t *bloom_entry;
	bool		is_triggered = false;
	LWLock *entry_lock;

	LWLockAcquire(bloom_set_lock, LW_SHARED);
	entry_lock = LWLockAcquireEntry(dbid, LW_SHARED);
	bloom_entry = find_bloom_entry(bloom_set, dbid);
	if (bloom_entry)
	{
		is_triggered = bloom_entry->bloom.is_set_all;
	}
	if (entry_lock)
		LWLockRelease(entry_lock);
	LWLockRelease(bloom_set_lock);

	return is_triggered;
}

int
bloom_set_count(bloom_set_t * bloom_set)
{
	int count = 0;
	bloom_entry_t *bloom_entry;

	LWLockAcquire(bloom_set_lock, LW_EXCLUSIVE);
	for (int i = 0; i < bloom_set->bloom_count; ++i)
	{
		bloom_entry = BLOOM_ENTRY_GET(bloom_set, i);
		if (bloom_entry->dbid != InvalidOid)
			++count;
	}
	LWLockRelease(bloom_set_lock);
	return count;
}
