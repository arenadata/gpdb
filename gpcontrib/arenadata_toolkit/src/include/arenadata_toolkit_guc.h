#ifndef ARENADATA_TOOLKIT_GUC_H
#define ARENADATA_TOOLKIT_GUC_H

#include "postgres.h"

extern int	bloom_size;
extern int	db_track_count;
extern int	drops_count;
extern bool get_full_snapshot_on_recovery;
extern char *tracked_schemas;
extern char *tracked_rel_storages;
extern char *tracked_rel_kinds;
extern int	tracking_worker_naptime_sec;

void		tf_guc_unlock_tracked_once(void);
void		tf_guc_unlock_full_snapshot_on_recovery_once(void);
void		tf_guc_define(void);
void		tf_guc_unlock_schemas_once(void);
void		tf_guc_unlock_relkinds_once(void);
void		tf_guc_unlock_relstorages_once(void);

#endif   /* ARENADATA_TOOLKIT_GUC_H */
