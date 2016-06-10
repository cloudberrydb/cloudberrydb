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
			CFileWriter m_fw;

			// private copy ctor
			COstreamFile(const COstreamFile &);
			
		public:

			// please see comments in COstream.h for an explanation
			using COstream::operator <<;
			
			// ctor
			COstreamFile
				(
				const CHAR *szPath,
				ULONG ulPerms = S_IRUSR | S_IWUSR
				);

			// dtor
			virtual
			~COstreamFile();

			// implement << operator				
			virtual
			IOstream& operator<< (const WCHAR *);
	};

}

#endif // !GPOS_COstreamFile_H

// EOF

