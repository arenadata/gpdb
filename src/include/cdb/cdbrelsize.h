/*-------------------------------------------------------------------------
 *
 * cdbrelsize.h
 *	  Get the size of the relation after decompression
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbrelsize.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBRELSIZE_H_
#define CDBRELSIZE_H_

#include "utils/relcache.h"

extern int64 cdbRelUncompressedSize(Relation rel);

#endif /* CDBRELSIZE_H_ */
