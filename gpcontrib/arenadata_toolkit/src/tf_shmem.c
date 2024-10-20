#include "tf_shmem.h"

#include "storage/ipc.h"
#include "storage/shmem.h"

#include "arenadata_toolkit_guc.h"
#include "bloom_set.h"

static shmem_startup_hook_type next_shmem_startup_hook = NULL;
tf_shared_state_t *tf_shared_state;

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

	tf_shared_state = ShmemInitStruct("toolkit_track_files", size, &found);

	if (!found)
	{
		tf_shared_state->is_initialized = false;
		tf_shared_state->has_error = false;
		tf_shared_state->state_lock = LWLockAssign();
		bloom_set_init(db_track_count, bloom_size, &tf_shared_state->bloom_set);
	}

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
