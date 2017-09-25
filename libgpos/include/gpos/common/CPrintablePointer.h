//	Copyright (C) 2017 Pivotal Software, Inc.

#ifndef GPOS_CPrintablePointer_H
#define GPOS_CPrintablePointer_H

#include "gpos/io/IOstream.h"

namespace gpos
{
	template <typename T>
	class CPrintablePointer
	{
		private:
			T *m_pt;
			friend IOstream &operator << (IOstream &os, CPrintablePointer p)
			{
				if (p.m_pt)
				{
					return os << *p.m_pt;
				}
				else
				{
					return os;
				}
			}

		public:
			explicit CPrintablePointer(T *pt) : m_pt(pt) {}
			CPrintablePointer(const CPrintablePointer &pointer) : m_pt(pointer.m_pt) {}
	};

	template <typename T>
	CPrintablePointer<T> pp(T *pt)
	{
		return CPrintablePointer<T>(pt);
	}
}
#endif // GPOS_CPrintablePointer_H
