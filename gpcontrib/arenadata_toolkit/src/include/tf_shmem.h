#ifndef TF_SHMEM_H
#define TF_SHMEM_H

#include "bloom_set.h"

typedef struct
{
	LWLock		*state_lock;
	bool		has_error;
	bool		is_initialized;
	bloom_set_t bloom_set;
}	tf_shared_state_t;

extern tf_shared_state_t * tf_shared_state;

void		tf_shmem_init(void);
void		tf_shmem_deinit(void);

#endif   /* TF_SHMEM_H */
