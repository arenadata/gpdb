//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDCache.h
//
//	@doc:
//		Metadata cache.
//---------------------------------------------------------------------------


#ifndef GPOPT_CMDCache_H
#define GPOPT_CMDCache_H

#include "gpos/base.h"
#include "gpos/memory/CCache.h"
#include "gpos/memory/CCacheFactory.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDKey.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@class:
//		CMDCache
//
//	@doc:
//		A wrapper for a generic cache to hide the details of metadata cache
//		creation and encapsulate a singleton cache object
//
//---------------------------------------------------------------------------
class CMDCache
{
private:
	// pointer to the underlying cache
	static CMDAccessor::MDCache *m_pcache;

	// the maximum size of the cache
	static ULLONG m_ullCacheQuota;

	// if we have cached a relation without an index, because that index cannot
	// be used in the current snapshot (for more info see src/backend/access/heap/README.HOT),
	// we save the TransactionXmin. If later TransactionXmin changes from
	// the saved value, the cache will be reset and the relation will be
	// reloaded with the index usage.
	static uint32_t m_xmin;

	// private ctor
	CMDCache(){};

	// no copy ctor
	CMDCache(const CMDCache &);

	// private dtor
	~CMDCache(){};

public:
	// initialize underlying cache
	static void Init();

	// has cache been initialized?
	static BOOL
	FInitialized()
	{
		return (NULL != m_pcache);
	}

	// destroy global instance
	static void Shutdown();

	// set the maximum size of the cache
	static void SetCacheQuota(ULLONG ullCacheQuota);

	// get the maximum size of the cache
	static ULLONG ULLGetCacheQuota();

	// get the number of times we evicted entries from this cache
	static ULLONG ULLGetCacheEvictionCounter();

	// reset global instance
	static void Reset();

	// global accessor
	static CMDAccessor::MDCache *
	Pcache()
	{
		return m_pcache;
	}

	static void
	MarkContainTemporaryRelation(uint32_t xmin)
	{
		m_xmin = xmin;
	}

	// get the transaction xmin in which the cache become temporary
	static uint32_t
	GetCacheTransactionXmin()
	{
		return m_xmin;
	}

};	// class CMDCache

}  // namespace gpopt

#endif	// !GPOPT_CMDCache_H

// EOF
