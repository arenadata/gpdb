#include "bloom_set.h"
#include "tf_shmem.h"

#include "storage/ipc.h"
#include "storage/shmem.h"

#include "arenadata_toolkit_guc.h"

static shmem_startup_hook_type next_shmem_startup_hook = NULL;
tf_shared_state_t *tf_shared_state;
LWLock *tf_state_lock;
LWLock *bloom_set_lock;
tf_entry_lock_t bloom_locks[MAX_DB_TRACK_COUNT];

static void
init_lwlocks(void)
{
	tf_state_lock = LWLockAssign();
	bloom_set_lock = LWLockAssign();

	for (int i = 0; i < db_track_count; ++i)
	{
		bloom_locks[i].lock = LWLockAssign();
		bloom_locks[i].dbid = InvalidOid;
	}
}

static Size
tf_shmem_calc_size(void)
{
	Size		size;

	size = offsetof(tf_shared_state_t, bloom_set);
	size = add_size(size, FULL_BLOOM_SET_SIZE(bloom_size, db_track_count));

	return size;
}

static void
tf_shmem_hook(void)
{
	bool		found;
	Size		size = tf_shmem_calc_size();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	tf_shared_state = ShmemInitStruct("toolkit_track_files", size, &found);

	if (!found)
	{
		pg_atomic_init_flag(&tf_shared_state->tracking_is_initialized);
		pg_atomic_init_flag(&tf_shared_state->tracking_error);
		bloom_set_init(db_track_count, bloom_size, &tf_shared_state->bloom_set);
	}

	init_lwlocks();

	LWLockRelease(AddinShmemInitLock);

	if (next_shmem_startup_hook)
		next_shmem_startup_hook();
}

void
tf_shmem_init()
{
	/* don't forget to add additional locks */
	RequestAddinLWLocks(2 + db_track_count);
	RequestAddinShmemSpace(tf_shmem_calc_size());

	next_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = tf_shmem_hook;
}

void
tf_shmem_deinit(void)
{
	shmem_startup_hook = next_shmem_startup_hook;
}

LWLock *
LWLockAcquireEntry(Oid dbid, LWLockMode mode)
{
	LWLockAcquire(tf_state_lock, LW_SHARED);
	for (int i = 0; i < db_track_count; ++i)
	{
		if (bloom_locks[i].dbid == dbid)
		{
			LWLockAcquire(bloom_locks[i].lock, mode);
			LWLockRelease(tf_state_lock);
			return bloom_locks[i].lock;
		}
	}
	LWLockRelease(tf_state_lock);

	return NULL;
}


void
LWLockBindEntry(Oid dbid)
{
	int i;
	LWLockAcquire(tf_state_lock, LW_EXCLUSIVE);
	for (i = 0; i < db_track_count; ++i)
	{
		if (bloom_locks[i].dbid == InvalidOid)
		{
			bloom_locks[i].dbid = dbid;
			break;
		}
	}

	if (i == db_track_count && pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_error))
		pg_atomic_test_set_flag(&tf_shared_state->tracking_error);
	LWLockRelease(tf_state_lock);
}

void
LWLockUnbindEntry(Oid dbid)
{
	int i;
	LWLockAcquire(tf_state_lock, LW_EXCLUSIVE);
	for (i = 0; i < db_track_count; ++i)
	{
		if (bloom_locks[i].dbid == dbid)
		{
			bloom_locks[i].dbid = InvalidOid;
			break;
		}
	}

	if (i == db_track_count && pg_atomic_unlocked_test_flag(&tf_shared_state->tracking_error))
		pg_atomic_test_set_flag(&tf_shared_state->tracking_error);

	LWLockRelease(tf_state_lock);
}
