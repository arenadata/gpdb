#ifndef BLOOM_H
#define BLOOM_H

#include <stdint.h>

#define FULL_BLOOM_SIZE(size) (offsetof(bloom_t, map) + size)

typedef struct
{
	uint32_t	size;			/* size in bytes of 'map' */
	int			is_set_all;		/* is all bits sets by bloom_set_all */
	char		map[] /* filter itself, array of bytes */ ;
}	bloom_t;

void		bloom_init(const uint32_t bloom_size, bloom_t *bloom);
int			bloom_isset(bloom_t * bloom, uint64_t hash);
void		bloom_set(bloom_t * bloom, uint64_t hash);
void		bloom_set_all(bloom_t * bloom);
void		bloom_clear(bloom_t * bloom);
void		bloom_merge(bloom_t * dst, bloom_t * src);
void		bloom_copy(bloom_t * src, bloom_t *dest);

#endif   /* BLOOM_H */
