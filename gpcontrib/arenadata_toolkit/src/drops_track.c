/*
 * Track unlink hook events.
 */

#include "drops_track.h"

#include "lib/ilist.h"
#include "storage/lwlock.h"
#include "storage/ipc.h"
#include "storage/shmem.h"

#include "arenadata_toolkit_guc.h"

#define TRACK_NODE_GET(track, i) (void *)(track->nodes + i * sizeof(drops_track_node_t));

typedef struct
{
	Oid			relNode;
	Oid			dbNode;
}	track_relfilenode_t;

/* doubly linked list node of dropped file nodes */
typedef struct
{
	dlist_node	node;
	uint32_t	idx;			/* idx in 'nodes' array; just for info */
	track_relfilenode_t relfileNode;
}	drops_track_node_t;


typedef struct
{
	LWLock	   *lock;
	dlist_head	head;
	uint32_t	used_count;		/* count of used nodes */
	int			unused_idx;		/* next unused idx or -1 if unknown; for
								 * faster search */
	char		nodes[FLEXIBLE_ARRAY_MEMBER];	/* array of drops_track_node_t */
}	drops_track_t;

static shmem_startup_hook_type next_shmem_startup_hook = NULL;
static drops_track_t * drops_track;

static Size
drops_track_calc_size()
{
	Size		size;

	size = offsetof(drops_track_t, nodes);
	size = add_size(size, mul_size(drops_count, sizeof(drops_track_node_t)));

	return size;
}

static void
drops_track_hook(void)
{
	bool		found;
	Size		size = drops_track_calc_size();

	drops_track = ShmemInitStruct("adb_track_files_drops", size, &found);

	if (!found)
	{
		drops_track->lock = LWLockAssign();
		drops_track->used_count = 0;
		drops_track->unused_idx = 0;
		dlist_init(&drops_track->head);

		for (uint32_t i = 0; i < drops_count; i++)
		{
			drops_track_node_t *track_node = TRACK_NODE_GET(drops_track, i);

			track_node->relfileNode.relNode = InvalidOid;
			track_node->relfileNode.dbNode = InvalidOid;
			track_node->idx = i;
		}
	}

	if (next_shmem_startup_hook)
		next_shmem_startup_hook();
}

void
drops_track_init(void)
{
	RequestAddinLWLocks(1);
	RequestAddinShmemSpace(drops_track_calc_size());

	next_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = drops_track_hook;
}

void
drops_track_deinit(void)
{
	shmem_startup_hook = next_shmem_startup_hook;
}

/* find unused node; this should be heavily reworked or optimized */
static drops_track_node_t * find_empty_node()
{
	drops_track_node_t *track_node = NULL;

	if (drops_track->unused_idx >= 0)
	{
		track_node = TRACK_NODE_GET(drops_track, drops_track->unused_idx);
		drops_track->unused_idx++;
		if (drops_track->unused_idx >= drops_count)
			drops_track->unused_idx = -1;
		else
		{
			drops_track_node_t *unused_node = TRACK_NODE_GET(drops_track, drops_track->unused_idx);

			if (unused_node->relfileNode.relNode != InvalidOid)
				drops_track->unused_idx = -1;
		}
	}
	else
	{
		for (uint32_t i = 0; i < drops_count; i++)
		{
			track_node = TRACK_NODE_GET(drops_track, i);
			if (track_node->relfileNode.relNode == InvalidOid)
				break;
		}
	}
	return track_node;
}

/* add relNode to track; old node is dropped if no space */
void
drops_track_add(RelFileNode relfileNode)
{
	drops_track_node_t *track_node;

	LWLockAcquire(drops_track->lock, LW_EXCLUSIVE);

	if (drops_track->used_count >= drops_count)
	{
		track_node = (drops_track_node_t *) dlist_pop_head_node(&drops_track->head);
		elog(DEBUG1, "No space for drop track. Oldest node removed (%d).", track_node->relfileNode.relNode);
	}
	else
	{
		track_node = find_empty_node();
		drops_track->used_count++;
		Assert(track_node);
	}

	track_node->relfileNode.relNode = relfileNode.relNode;
	track_node->relfileNode.dbNode = relfileNode.dbNode;
	dlist_push_tail(&drops_track->head, &track_node->node);

	LWLockRelease(drops_track->lock);
}

/* move relfilenodes from track to list */
List *
drops_track_move(Oid dbid)
{
	List	   *oids = NIL;
	dlist_mutable_iter iter;

	LWLockAcquire(drops_track->lock, LW_EXCLUSIVE);

	if (drops_track->used_count == 0)
	{
		LWLockRelease(drops_track->lock);
		return oids;
	}

	dlist_foreach_modify(iter, &drops_track->head)
	{
		drops_track_node_t *track_node = (drops_track_node_t *) iter.cur;

		/* newest in head, oldest in tail */
		if (track_node->relfileNode.dbNode == dbid)
		{
			oids = lcons_oid(track_node->relfileNode.relNode, oids);
			drops_track->used_count--;
			track_node->relfileNode.relNode = InvalidOid;
			track_node->relfileNode.dbNode = InvalidOid;
			dlist_delete(&track_node->node);
		}
	}

	LWLockRelease(drops_track->lock);

	return oids;
}

/* undo moving of relfilenodes; old nodes are dropped if no space */
void
drops_track_move_undo(List *oids, Oid dbid)
{
	ListCell   *cell;

	if (oids == NIL)
		return;

	LWLockAcquire(drops_track->lock, LW_EXCLUSIVE);

	foreach(cell, oids)
	{
		Oid			oid = lfirst_oid(cell);
		drops_track_node_t *track_node;

		if (drops_track->used_count >= drops_count)
		{
			elog(DEBUG1, "No space for move back. Oldest node removed (%d).", oid);
			continue;
		}

		track_node = find_empty_node();
		drops_track->used_count++;
		track_node->relfileNode.relNode = oid;
		track_node->relfileNode.dbNode = dbid;
		dlist_push_head(&drops_track->head, &track_node->node);
	}

	LWLockRelease(drops_track->lock);
}
