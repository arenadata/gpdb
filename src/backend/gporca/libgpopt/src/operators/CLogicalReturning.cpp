//---------------------------------------------------------------------------
//	Copyright (c) 2025 Greengage Community
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
	: CLogical(mp), m_ptabdesc(NULL), m_pdrgpcrOutput(NULL)
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
	: CLogical(mp), m_ptabdesc(ptabdesc), m_pdrgpcrOutput(NULL)
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

//---------------------------------------------------------------------------
//	@function:
//		CLogicalReturning::~CLogicalReturning
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalReturning::~CLogicalReturning()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalReturning::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalReturning::MatchesReturning(CLogicalReturning *popReturning) const
{
	return m_ptabdesc->MDId()->Equals(popReturning->Ptabdesc()->MDId()) &&
		   m_pdrgpcrOutput->Equals(popReturning->PdrgpcrOutput());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalReturning::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalReturning::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(),
									   m_ptabdesc->MDId()->HashValue());

	ulHash =
		gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalReturning::CopyRemappedColumns
//
//	@doc:
//		return a copy of output columns
//
//---------------------------------------------------------------------------
CColRefArray *
CLogicalReturning::CopyRemappedColumns(CMemoryPool *mp,
									   UlongToColRefMap *colref_mapping,
									   BOOL must_exist)
{
	CColRefArray *pdrgpcrOutput = NULL;
	if (must_exist)
	{
		pdrgpcrOutput =
			CUtils::PdrgpcrRemapAndCreate(mp, m_pdrgpcrOutput, colref_mapping);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOutput,
											 colref_mapping, must_exist);
	}

	return pdrgpcrOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalReturning::DeriveKeyCollection
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalReturning::DeriveKeyCollection(CMemoryPool *mp,
									   CExpressionHandle &	// exprhdl
) const
{
	const CBitSetArray *pdrgpbs = m_ptabdesc->PdrgpbsKeys();

	return CLogical::PkcKeysBaseTable(mp, pdrgpbs, m_pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalReturning::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
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
