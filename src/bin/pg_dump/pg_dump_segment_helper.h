/*-------------------------------------------------------------------------
 *
 * pg_dump_segment_helper.h
 *	  pg_dump_segment_helper provides public interface for saving data
 *	  from stdin and restoring data to stdout
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_DUMP_SEGMENT_HELPER
#define PG_DUMP_SEGMENT_HELPER

#include "pg_backup.h"

extern int onSegmentHelper_RunRestore(const char *inputFileSpec, int compressLevel, ArchiveFormat format);
extern int onSegmentHelper_RunDump(const char *inputFileSpec, int compressLevel, ArchiveFormat format);

#endif // PG_DUMP_SEGMENT_HELPER

