//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncHashtableAccessByIter.h
//
//	@doc:
//		Iterator's accessor provides protected access to hashtable elements
//		during iteration.
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtableAccessByIter_H
#define GPOS_CSyncHashtableAccessByIter_H


#include "gpos/common/CSyncHashtableAccessorBase.h"
#include "gpos/common/CSyncHashtableIter.h"


namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncHashtableAccessByIter<T, K, S>
	//
	//	@doc:
	//		Accessor class to provide access to the element pointed to by a
	//		hash table iterator
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	class CSyncHashtableAccessByIter : public CSyncHashtableAccessorBase<T, K, S>
	{

		// iterator class is a friend
		friend class CSyncHashtableIter<T, K, S>;

		private:

			// shorthand for base class
			typedef class CSyncHashtableAccessorBase<T, K, S> Base;

			// target iterator
			CSyncHashtableIter<T, K, S> &m_iter;

			// no copy ctor
			CSyncHashtableAccessByIter<T, K, S>
				(const CSyncHashtableAccessByIter<T, K, S>&);

			// returns the first valid element starting from the given element
			T *PtFirstValid(T *pt) const;

		public:

			// ctor
			explicit
			CSyncHashtableAccessByIter<T, K, S>
				(CSyncHashtableIter<T, K, S> &iter);

			// returns the element pointed to by iterator
			T *Pt() const;

	}; // class CSyncHashtableAccessByIter

}

// include implementation
#include "gpos/common/CSyncHashtableAccessByIter.inl"

#endif // !GPOS_CSyncHashtableAccessByIter_H

// EOF
