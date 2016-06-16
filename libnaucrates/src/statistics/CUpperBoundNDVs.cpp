//---------------------------------------------------------------------------
//      Greenplum Database
//      Copyright (C) 2014 Pivotal Inc.
//
//      @filename:
//              CUpperBoundNDVs.cpp
//
//      @doc:
//              Implementation of upper bound on the number of distinct values for a
//              given set of columns
//---------------------------------------------------------------------------

#include "naucrates/statistics/CUpperBoundNDVs.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//      @function:
//              CUpperBoundNDVs::PubndvCopyWithRemap
//
//      @doc:
//              Copy upper bound ndvs with remapped column id; function will
//              return null if there is no mapping found for any of the columns
//
//---------------------------------------------------------------------------
CUpperBoundNDVs *
CUpperBoundNDVs::PubndvCopyWithRemap
        (
        IMemoryPool *pmp,
        HMUlCr *phmulcr
        )
        const
{
        BOOL fMappingNotFound = false;

        CColRefSet *pcrsCopy = GPOS_NEW(pmp) CColRefSet(pmp);
        CColRefSetIter crsi(*m_pcrs);
        while (crsi.FAdvance() && !fMappingNotFound)
        {
                ULONG ulColId = crsi.Pcr()->UlId();
                CColRef *pcrNew = phmulcr->PtLookup(&ulColId);
                if (NULL != pcrNew)
                {
                        pcrsCopy->Include(pcrNew);
                }
               else
                {
                        fMappingNotFound = true;
                }
        }

        if (0 < pcrsCopy->CElements() && !fMappingNotFound)
        {
                return GPOS_NEW(pmp) CUpperBoundNDVs(pcrsCopy, DUpperBoundNDVs());
        }

        pcrsCopy->Release();

        return NULL;
}


//---------------------------------------------------------------------------
//      @function:
//              CUpperBoundNDVs::PubndvCopy
//
//      @doc:
//              Copy upper bound ndvs
//
//---------------------------------------------------------------------------
CUpperBoundNDVs *
CUpperBoundNDVs::PubndvCopy
        (
        IMemoryPool *pmp,
        CDouble dUpperBoundNDVs
       )
        const
{
        m_pcrs->AddRef();
        CUpperBoundNDVs *pndvCopy = GPOS_NEW(pmp) CUpperBoundNDVs(m_pcrs, dUpperBoundNDVs);

        return pndvCopy;
}

//---------------------------------------------------------------------------
//      @function:
//              CUpperBoundNDVs::PubndvCopy
//
//      @doc:
//              Copy upper bound ndvs
//
//---------------------------------------------------------------------------
CUpperBoundNDVs *
CUpperBoundNDVs::PubndvCopy
        (
        IMemoryPool *pmp
        )
        const
{
        return PubndvCopy(pmp, m_dUpperBoundNDVs);
}


//---------------------------------------------------------------------------
//      @function:
//              CUpperBoundNDVs::OsPrint
//
//      @doc:
//              Print function
//
//---------------------------------------------------------------------------
IOstream &
CUpperBoundNDVs::OsPrint
        (
        IOstream &os
        )
        const
{
        os << "{" << std::endl;
        m_pcrs->OsPrint(os);
        os << " Upper Bound of NDVs" << DUpperBoundNDVs() << std::endl;
        os << "}" << std::endl;

        return os;
}

// EOF


