//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalReturning.h
//
//	@doc:
//		Base class of operators that have returning columns
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalReturning_H
#define GPOS_CLogicalReturning_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "naucrates/base/IDatum.h"

namespace gpopt
{
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@class:
//		CLogicalReturning
//
//	@doc:
//		Base class of logical operators that have returning columns
//
//---------------------------------------------------------------------------
class CLogicalReturning : public CLogical
{
private:
	// private copy ctor
	CLogicalReturning(const CLogicalReturning &);

protected:
	// table descriptor
	CTableDescriptor *m_ptabdesc;

	// returning columns
	CColRefArray *m_pdrgpcrOutput;

public:
	// ctor
	CLogicalReturning(CMemoryPool *mp);

	// ctor
	CLogicalReturning(CMemoryPool *mp, CTableDescriptor *ptabdesc);

	// ctor
	CLogicalReturning(CMemoryPool *mp, CTableDescriptor *ptabdesc,
					  CColRefArray *pdrgpcrOutput);

	// dtor
	virtual ~CLogicalReturning();

	// output columns
	CColRefArray *
	PdrgpcrOutput() const
	{
		return m_pdrgpcrOutput;
	}

	// return table's descriptor
	CTableDescriptor *
	Ptabdesc() const
	{
		return m_ptabdesc;
	}

	// operator specific hash function
	virtual ULONG HashValue() const;

	// match function
	virtual BOOL MatchesReturning(CLogicalReturning *popReturning) const;

	// derive key collections
	virtual CKeyCollection *DeriveKeyCollection(
		CMemoryPool *mp, CExpressionHandle &exprhdl) const;

	// debug print
	virtual IOstream &OsPrint(IOstream &) const;

};	// class CLogicalReturning

}  // namespace gpopt

#endif	// !GPOS_CLogicalReturning_H

// EOF
