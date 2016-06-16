//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename: 
//		CAutoRg.h
//
//	@doc:
//		Basic auto range implementation; do not anticipate ownership based
//		on assignment to other auto ranges etc. Require explicit return/assignment
//		to re-init the object;
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoRg_H
#define GPOS_CAutoRg_H

#include "gpos/base.h"
#include "gpos/common/CStackObject.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoRg
	//
	//	@doc:
	//		Wrapper around arrays analogous to CAutoP
	//
	//---------------------------------------------------------------------------
	template <class T>
	class CAutoRg : public CStackObject
	{
		
		private:

			// actual element to point to
			T *m_rgt;
						
			// hidden copy ctor
			CAutoRg<T>(const CAutoRg&);

		public:
		
			// ctor
			explicit
			CAutoRg<T>()
				:
				m_rgt(NULL)
			{}

			// ctor
			explicit
			CAutoRg<T>(T *rgt)
				:
				m_rgt(rgt)
			{}


			// dtor
			virtual ~CAutoRg();

			// simple assignment
			inline
			CAutoRg<T> const & operator = (T* rgt)
			{
				m_rgt = rgt;
				return *this;
			}

			// indexed access
			inline
			T& operator []
				(
				ULONG ulPos
				)
			{
				return m_rgt[ulPos];
			}
			
			// return basic pointer
			T* Rgt()
			{
				return m_rgt;
			}

			// unhook pointer from auto object
			inline
			T* RgtReset()
			{
				T* rgt = m_rgt;
				m_rgt = NULL;
				return rgt;
			}

	}; // class CAutoRg


	//---------------------------------------------------------------------------
	//	@function:
	//		CAutoRg::~CAutoRg
	//
	//	@doc:
	//		Dtor
	//
	//---------------------------------------------------------------------------
	template <class T>
	CAutoRg<T>::~CAutoRg()
	{
		GPOS_DELETE_ARRAY(m_rgt);
	}
}

#endif // !GPOS_CAutoRg_H

// EOF

