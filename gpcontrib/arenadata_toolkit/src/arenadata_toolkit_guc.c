#include "arenadata_toolkit_guc.h"


#include "cdb/cdbvars.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_db_role_setting.h"
#include <limits.h>
#include "utils/guc.h"
#include "tf_shmem.h"

#define DEFAULT_BLOOM_SIZE 1000000
#define DEFAULT_DB_TRACK_COUNT 5
#define DEFAULT_IS_TRACKED false
#define DEFAULT_DROPS_COUNT 100000
#define DEFAULT_TRACKED_SCHEMAS "public,arenadata_toolkit,pg_catalog,pg_toast,pg_aoseg,information_schema"
#define DEFAULT_GET_FULL_SNAPSHOT_ON_RECOVERY true
#define DEFAULT_TRACKED_REL_STORAGES "h,a,c"
#define DEFAULT_TRACKED_REL_KINDS "r,i,t,m,o,b,M"
#define DEFAULT_NAPTIME 60

#define MIN_BLOOM_SIZE 1
#define MIN_DB_TRACK_COUNT 1
#define MIN_DROPS_COUNT 1
#define MIN_NAPTIME 1

#define MAX_BLOOM_SIZE 128000000
#define MAX_DB_TRACK_COUNT 1000
#define MAX_DROPS_COUNT 1000000
#define MAX_NAPTIME OID_MAX & 0x7FFFFFFF

int			bloom_size = DEFAULT_BLOOM_SIZE;
int			db_track_count = DEFAULT_DB_TRACK_COUNT;
bool		is_tracked = DEFAULT_IS_TRACKED;
bool		get_full_snapshot_on_recovery = DEFAULT_GET_FULL_SNAPSHOT_ON_RECOVERY;
int			drops_count = DEFAULT_DROPS_COUNT;
char	   *tracked_schemas = DEFAULT_TRACKED_SCHEMAS;
char	   *tracked_rel_storages = DEFAULT_TRACKED_REL_STORAGES;
char	   *tracked_rel_kinds = DEFAULT_TRACKED_REL_KINDS;
int			tracking_worker_naptime_sec = DEFAULT_NAPTIME;

static bool is_tracked_unlocked = false;
static bool is_get_full_snapshot_on_recovery_unlocked = false;
static bool is_schemas_unlocked = false;
static bool is_relkinds_unlocked = false;
static bool is_relstorages_unlocked = false;

void
tf_guc_unlock_tracked_once(void)
{
	if (!is_tracked_unlocked)
		is_tracked_unlocked = true;
}

void
tf_guc_unlock_full_snapshot_on_recovery_once(void)
{
	if (!is_get_full_snapshot_on_recovery_unlocked)
		is_get_full_snapshot_on_recovery_unlocked = true;
}

void
tf_guc_unlock_schemas_once(void)
{
	if (!is_schemas_unlocked)
		is_schemas_unlocked = true;
}

void
tf_guc_unlock_relkinds_once(void)
{
	if (!is_relkinds_unlocked)
		is_relkinds_unlocked = true;
}

void
tf_guc_unlock_relstorages_once(void)
{
	if (!is_relstorages_unlocked)
		is_relstorages_unlocked = true;
}

/* Prohibit changing the GUC value manually except several cases.
 * This is not called for RESET, so RESET is not guarded
 */
static bool
check_tracked(bool *newval, void **extra, GucSource source)
{
	if (IsInitProcessingMode() || Gp_role == GP_ROLE_EXECUTE ||
		(Gp_role == GP_ROLE_DISPATCH && is_tracked_unlocked))
	{
		if (is_tracked_unlocked)
			is_tracked_unlocked = false;

		if (source != PGC_S_DATABASE && source != PGC_S_DEFAULT && source != PGC_S_TEST)
			return false;

		return true;
	}

	GUC_check_errmsg("cannot change tracking status outside the tracking_register_db function");
	return false;
}

/* Prohibit changing the GUC value manually except several cases.
 * This is not called for RESET, so RESET is not guarded
 */
static bool
check_get_full_snapshot_on_recovery(bool *newval, void **extra, GucSource source)
{
	if (IsInitProcessingMode() || Gp_role == GP_ROLE_EXECUTE ||
		(Gp_role == GP_ROLE_DISPATCH && is_get_full_snapshot_on_recovery_unlocked))
	{
		if (is_get_full_snapshot_on_recovery_unlocked)
			is_get_full_snapshot_on_recovery_unlocked = false;

		if (source != PGC_S_DATABASE && source != PGC_S_DEFAULT && source != PGC_S_TEST)
			return false;

		return true;
	}

	GUC_check_errmsg("cannot change tracking status outside the tracking_set_snapshot_on_recovery function");
	return false;
}

static bool
check_relkinds(char **newval, void **extra, GucSource source)
{
	if (IsInitProcessingMode() || Gp_role == GP_ROLE_EXECUTE ||
		(Gp_role == GP_ROLE_DISPATCH && is_relkinds_unlocked))
	{
		if (is_relkinds_unlocked)
			is_relkinds_unlocked = false;

		if (source != PGC_S_DATABASE && source != PGC_S_DEFAULT && source != PGC_S_TEST)
			return false;

		return true;
	}

	GUC_check_errmsg("cannot change tracking status outside the tracking_register_relkinds function");
	return false;
}

static bool
check_schemas(char **newval, void **extra, GucSource source)
{
	if (IsInitProcessingMode() || Gp_role == GP_ROLE_EXECUTE ||
		(Gp_role == GP_ROLE_DISPATCH && is_schemas_unlocked))
	{
		if (is_schemas_unlocked)
			is_schemas_unlocked = false;

		if (source != PGC_S_DATABASE && source != PGC_S_DEFAULT && source != PGC_S_TEST)
			return false;

		return true;
	}

	GUC_check_errmsg("cannot change tracking status outside the tracking_register_schema function");
	return false;
}

static bool
check_relstorages(char **newval, void **extra, GucSource source)
{
	if (IsInitProcessingMode() || Gp_role == GP_ROLE_EXECUTE ||
		(Gp_role == GP_ROLE_DISPATCH && is_relstorages_unlocked))
	{
		if (is_relstorages_unlocked)
			is_relstorages_unlocked = false;

		if (source != PGC_S_DATABASE && source != PGC_S_DEFAULT && source != PGC_S_TEST)
			return false;

		return true;
	}

	GUC_check_errmsg("cannot change tracking status outside the tracking_register_relstorages function");
	return false;
}

void
tf_guc_define(void)
{
	DefineCustomIntVariable("arenadata_toolkit.tracking_bloom_size",
				   "Size of bloom filter in bytes for each tracked database",
							NULL,
							&bloom_size,
							DEFAULT_BLOOM_SIZE,
							MIN_BLOOM_SIZE,
							MAX_BLOOM_SIZE,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL
		);

	DefineCustomIntVariable("arenadata_toolkit.tracking_db_track_count",
							"Count of tracked databases.",
							NULL,
							&db_track_count,
							DEFAULT_DB_TRACK_COUNT,
							MIN_DB_TRACK_COUNT,
							MAX_DB_TRACK_COUNT,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL
		);

	DefineCustomBoolVariable("arenadata_toolkit.tracking_is_db_tracked",
							 "Is current database tracked.",
							 NULL,
							 &is_tracked,
							 DEFAULT_IS_TRACKED,
							 PGC_SUSET,
							 0,
							 &check_tracked,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("arenadata_toolkit.tracking_snapshot_on_recovery",
							 "Return full snapshot at startup/recovery.",
							 NULL,
							 &get_full_snapshot_on_recovery,
							 DEFAULT_GET_FULL_SNAPSHOT_ON_RECOVERY,
							 PGC_SUSET,
							 0,
							 &check_get_full_snapshot_on_recovery,
							 NULL,
							 NULL);

	DefineCustomIntVariable("arenadata_toolkit.tracking_drops_count",
							"Count of max monitored drop events.",
							NULL,
							&drops_count,
							DEFAULT_DROPS_COUNT,
							MIN_DROPS_COUNT,
							MAX_DROPS_COUNT,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("arenadata_toolkit.tracking_schemas",
							   "Tracked schema names.",
							   NULL,
							   &tracked_schemas,
							   DEFAULT_TRACKED_SCHEMAS,
							   PGC_SUSET,
							   0,
							   &check_schemas,
							   NULL,
							   NULL);

	DefineCustomStringVariable("arenadata_toolkit.tracking_relstorages",
							   "Tracked relation storage types.",
							   NULL,
							   &tracked_rel_storages,
							   DEFAULT_TRACKED_REL_STORAGES,
							   PGC_SUSET,
							   0,
							   &check_relstorages,
							   NULL,
							   NULL);

	DefineCustomStringVariable("arenadata_toolkit.tracking_relkinds",
							   "Tracked relation kinds.",
							   NULL,
							   &tracked_rel_kinds,
							   DEFAULT_TRACKED_REL_KINDS,
							   PGC_SUSET,
							   0,
							   &check_relkinds,
							   NULL,
							   NULL);


	DefineCustomIntVariable("arenadata_toolkit.tracking_worker_naptime_sec",
							"Toolkit background worker nap time",
							NULL,
							&tracking_worker_naptime_sec,
							DEFAULT_NAPTIME,
							1,
							MAX_NAPTIME,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
}
