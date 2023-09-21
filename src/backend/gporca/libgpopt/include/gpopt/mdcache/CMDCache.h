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

	// is cached data transient
	static BOOL m_transient;

	// in which transaction did it become transient
	static uint32_t m_transientXmin;

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

	// mark cache as transient
	static void
	SetTransient(uint32_t transientXmin)
	{
		m_transient = true;
		m_transientXmin = transientXmin;
	}

	// get transaction id in which did it become transient
	static uint32_t
	GetTransientXmin()
	{
		return m_transientXmin;
	}

	// get is cached data transient
	static BOOL
	IsTransient()
	{
		return m_transient;
	}

};	// class CMDCache

}  // namespace gpopt

#endif	// !GPOPT_CMDCache_H

// EOF
