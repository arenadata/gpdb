#ifndef TF_SHMEM_H
#define TF_SHMEM_H

#include "storage/lwlock.h"

#include "bloom_set.h"

typedef struct
{
	bool		has_error;
	bool		is_initialized;
	bloom_set_t bloom_set;
}	tf_shared_state_t;

typedef struct
{
	Oid dbid;
	LWLock *lock;
} tf_entry_lock_t;

extern tf_shared_state_t * tf_shared_state;
extern LWLock *tf_state_lock;
extern LWLock *bloom_set_lock;
extern tf_entry_lock_t bloom_locks[];

void		tf_shmem_init(void);
void		tf_shmem_deinit(void);
LWLock *		LWLockAcquireEntry(Oid dbid, LWLockMode mode);
void		LWLockBindEntry(Oid dbid);
void		LWLockUnbindEntry(Oid dbid);

#endif   /* TF_SHMEM_H */
