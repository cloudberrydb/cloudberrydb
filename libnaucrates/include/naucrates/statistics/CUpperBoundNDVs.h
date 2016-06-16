//---------------------------------------------------------------------------
//      Greenplum Database
//      Copyright (C) 2014 Pivotal Inc.
//
//      @filename:
//              CUpperBoundNDVs.h
//
//      @doc:
//              Upper bound on the number of distinct values for a given set of columns
//---------------------------------------------------------------------------

#ifndef GPNAUCRATES_CUpperBoundNDVs_H
#define GPNAUCRATES_CUpperBoundNDVs_H

#include "gpos/base.h"
#include "gpopt/base/CColRefSet.h"

namespace gpnaucrates
{
        using namespace gpos;
        using namespace gpmd;

        // forward decl
        class CUpperBoundNDVs;

        // dynamic array of upper bound ndvs
        typedef CDynamicPtrArray<CUpperBoundNDVs, CleanupDelete> DrgPubndvs;

        //---------------------------------------------------------------------------
        //      @class:
        //              CUpperBoundNDVs
        //
        //      @doc:
        //              Upper bound on the number of distinct values for a given set of columns
        //
        //---------------------------------------------------------------------------

        class CUpperBoundNDVs
        {
                private:

                        // set of column references
                        CColRefSet *m_pcrs;

                        // upper bound of ndvs
                        CDouble m_dUpperBoundNDVs;

                        // private copy constructor
                        CUpperBoundNDVs(const CUpperBoundNDVs &);

                public:

                        // ctor
                        CUpperBoundNDVs
                                (
                                CColRefSet *pcrs,
                                CDouble dUpperBoundNDVs
                                )
                                :
                                m_pcrs(pcrs),
                                m_dUpperBoundNDVs(dUpperBoundNDVs)
                        {
                                GPOS_ASSERT(NULL != pcrs);
                        }

                        // dtor
                        ~CUpperBoundNDVs()
                        {
                                m_pcrs->Release();
                        }

                        // return the upper bound of ndvs
                        CDouble DUpperBoundNDVs() const
                        {
                                return m_dUpperBoundNDVs;
                        }

                        // check if the column is present
                        BOOL FPresent(const CColRef *pcr) const
                        {
                                return m_pcrs->FMember(pcr);
                        }

                        // copy upper bound ndvs
                        CUpperBoundNDVs *PubndvCopy(IMemoryPool *pmp) const;
                        CUpperBoundNDVs *PubndvCopy(IMemoryPool *pmp, CDouble dUpperBoundNDVs) const;

                        // copy upper bound ndvs with remapped column id; function will
                        // return null if there is no mapping found for any of the columns
                        CUpperBoundNDVs *PubndvCopyWithRemap(IMemoryPool *pmp, HMUlCr *phmulcr) const;

                        // print function
                        IOstream &OsPrint(IOstream &os) const;
        };
}

#endif // !GPNAUCRATES_CUpperBoundNDVs_H

// EOF


