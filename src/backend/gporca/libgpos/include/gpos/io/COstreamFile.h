//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		COstreamFile.h
//
//	@doc:
//		Output file stream class;
//---------------------------------------------------------------------------
#ifndef GPOS_COstreamFile_H
#define GPOS_COstreamFile_H

#include "gpos/io/ioutils.h"
#include "gpos/io/CFileWriter.h"
#include "gpos/io/COstream.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		COstreamFile
//
//	@doc:
//		Implements an output stream writing to a file
//
//---------------------------------------------------------------------------
class COstreamFile : public COstream
{
private:
	// underlying file writer
	CFileWriter m_file_writer;

public:
	COstreamFile(const COstreamFile &) = delete;

	// please see comments in COstream.h for an explanation
	using COstream::operator<<;

	// ctor
	COstreamFile(const CHAR *file_path,
				 ULONG permission_bits = S_IRUSR | S_IWUSR);

	// dtor
	~COstreamFile() override;

	// implement << operator
	IOstream &operator<<(const WCHAR *) override;
};

}  // namespace gpos

#endif	// !GPOS_COstreamFile_H

// EOF
