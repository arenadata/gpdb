//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLTableDescr.h
//
//	@doc:
//		Class for representing table descriptors.
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLTableDescriptor_H
#define GPDXL_CDXLTableDescriptor_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLColDescr.h"
#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDId.h"

// default value for m_assigned_query_id - no assigned query for table descriptor
#define UNASSIGNED_QUERYID 0

namespace gpdxl
{
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CDXLTableDescr
//
//	@doc:
//		Class for representing table descriptors in a DXL tablescan node.
//
//---------------------------------------------------------------------------
class CDXLTableDescr : public CRefCount
{
private:
	// memory pool
	CMemoryPool *m_mp;

	// id and version information for the table
	IMDId *m_mdid;

	// table name
	CMDName *m_mdname;

	// list of column descriptors
	CDXLColDescrArray *m_dxl_column_descr_array;

	// id of user the table needs to be accessed with
	ULONG m_execute_as_user_id;

	// identifier of query to which current table belongs.
	// This field is used for assigning current table entry with
	// target one within DML operation
	ULONG m_assigned_query_id;

	// private copy ctor
	CDXLTableDescr(const CDXLTableDescr &);

	void SerializeMDId(CXMLSerializer *xml_serializer) const;

public:
	// ctor/dtor
	CDXLTableDescr(CMemoryPool *mp, IMDId *mdid, CMDName *mdname,
				   ULONG ulExecuteAsUser,
				   ULONG assigned_query_id = UNASSIGNED_QUERYID);

	virtual ~CDXLTableDescr();

	// setters
	void SetColumnDescriptors(CDXLColDescrArray *dxl_column_descr_array);

	void AddColumnDescr(CDXLColDescr *pdxlcd);

	// table name
	const CMDName *MdName() const;

	// table mdid
	IMDId *MDId() const;

	// table arity
	ULONG Arity() const;

	// user id
	ULONG GetExecuteAsUserId() const;

	// get the column descriptor at the given position
	const CDXLColDescr *GetColumnDescrAt(ULONG idx) const;

	// serialize to dxl format
	void SerializeToDXL(CXMLSerializer *xml_serializer) const;

	// assigned query id
	ULONG GetAssignedQueryId() const;
};
}  // namespace gpdxl


#endif	// !GPDXL_CDXLTableDescriptor_H

// EOF
