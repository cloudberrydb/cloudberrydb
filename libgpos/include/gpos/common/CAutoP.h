//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename: 
//		CAutoP.h
//
//	@doc:
//		Basic auto pointer implementation; do not anticipate ownership based
//		on assignment to other auto pointers etc. Require explicit return/assignment
//		to re-init the object;
//
//		This is primarily used for auto destruction.
//		Not using Boost's auto pointer logic to discourage blurring ownership
//		of objects.
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoP_H
#define GPOS_CAutoP_H

#include "gpos/base.h"
#include "gpos/common/CStackObject.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoP
	//
	//	@doc:
	//		Wrapps pointer of type T; overloads *, ->, = does not provide
	//		copy ctor;
	//
	//---------------------------------------------------------------------------
	template <class T>
	class CAutoP : public CStackObject
	{

		protected:

			// actual element to point to
			T *m_pt;
						
			// hidden copy ctor
			CAutoP<T>
				(
				const CAutoP&
				);

		public:
		
			// ctor
			explicit
			CAutoP<T>()
				:
				m_pt(NULL)
			{}

			explicit
			CAutoP<T>(T *pt)
				:
				m_pt(pt)
			{}

			// dtor
			virtual ~CAutoP();

			// simple assignment
			CAutoP<T> const & operator = (T* pt)
			{
				m_pt = pt;
				return *this;
			}

			// deref operator
			T &operator * ()
			{
				GPOS_ASSERT(NULL != m_pt);
				return *m_pt;
			}
			
			// returns only base pointer, compiler does appropriate deref'ing
			T* operator -> ()
			{
				return m_pt;
			}

			// return basic pointer
			T* Pt() 
			{
				return m_pt;
			}
			
			// unhook pointer from auto object
			T* PtReset()
			{
				T* pt = m_pt;
				m_pt = NULL;
				return pt;
			}

	}; // class CAutoP

	//---------------------------------------------------------------------------
	//	@function:
	//		CAutoP::~CAutoP
	//
	//	@doc:
	//		Dtor
	//
	//---------------------------------------------------------------------------
	template <class T>
	CAutoP<T>::~CAutoP()
	{
		GPOS_DELETE(m_pt);
	}
}


#endif // !GPOS_CAutoP_H

// EOF

