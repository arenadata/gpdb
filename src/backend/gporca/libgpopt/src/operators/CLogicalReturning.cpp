//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalReturning.cpp
//
//	@doc:
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalReturning.h"

#include "gpos/base.h"

#include "gpopt/xforms/CXformUtils.h"
#include "naucrates/statistics/CProjectStatsProcessor.h"

using namespace gpopt;

CLogicalReturning::CLogicalReturning(CMemoryPool *mp)
	: CLogical(mp),
	  m_ptabdesc(NULL),
	  m_pdrgpcrOutput(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::CLogicalDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalReturning::CLogicalReturning(CMemoryPool *mp,
						CTableDescriptor *ptabdesc)
	: CLogical(mp),
	  m_ptabdesc(ptabdesc),
	  m_pdrgpcrOutput(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);

	m_pdrgpcrOutput =
		PdrgpcrCreateMapping(mp, ptabdesc->Pdrgpcoldesc(), UlOpId());

	m_pcrsLocalUsed->Include(m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDML::CLogicalDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalReturning::CLogicalReturning(CMemoryPool *mp,
									 CTableDescriptor *ptabdesc,
									 CColRefArray *pdrgpcrOutput)
	: CLogical(mp), m_ptabdesc(ptabdesc), m_pdrgpcrOutput(pdrgpcrOutput)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrOutput);

	m_pcrsLocalUsed->Include(m_pdrgpcrOutput);
}

CLogicalReturning::~CLogicalReturning()
{
	CRefCount::SafeRelease(m_pdrgpcrOutput);
}


IOstream &
CLogicalReturning::OsPrint(IOstream &os) const
{
	os << "Output Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "] Key sets: {";

	const ULONG ulColumns = m_pdrgpcrOutput->Size();
	const CBitSetArray *pdrgpbsKeys = m_ptabdesc->PdrgpbsKeys();
	for (ULONG ul = 0; ul < pdrgpbsKeys->Size(); ul++)
	{
		CBitSet *pbs = (*pdrgpbsKeys)[ul];
		if (0 < ul)
		{
			os << ", ";
		}
		os << "[";
		ULONG ulPrintedKeys = 0;
		for (ULONG ulKey = 0; ulKey < ulColumns; ulKey++)
		{
			if (pbs->Get(ulKey))
			{
				if (0 < ulPrintedKeys)
				{
					os << ",";
				}
				os << ulKey;
				ulPrintedKeys++;
			}
		}
		os << "]";
	}
	os << "}";

	return os;
}

// EOF
