//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumGenericGPDB.cpp
//
//	@doc:
//		Implementation of GPDB generic datum
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/base/CDatumGenericGPDB.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/CMDIdGPDB.h"

#include "naucrates/statistics/CScaleFactorUtils.h"

using namespace gpnaucrates;
using namespace gpmd;

// selectivities needed for LIKE predicate statistics evaluation
const CDouble CDatumGenericGPDB::DDefaultFixedCharSelectivity(0.20);
const CDouble CDatumGenericGPDB::DDefaultCharRangeSelectivity(0.25);
const CDouble CDatumGenericGPDB::DDefaultAnyCharSelectivity(0.99);
const CDouble CDatumGenericGPDB::DDefaultCdbRanchorSelectivity(0.95);
const CDouble CDatumGenericGPDB::DDefaultCdbRolloffSelectivity(0.14);

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::CDatumGenericGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumGenericGPDB::CDatumGenericGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	INT iTypeModifier,
	const void *pv,
	ULONG ulSize,
	BOOL fNull,
	LINT lValue,
	CDouble dValue
	)
	:
	m_pmp(pmp),
	m_ulSize(ulSize),
	m_pbVal(NULL),
	m_fNull(fNull),
	m_pmdid(pmdid),
	m_iTypeModifier(iTypeModifier),
	m_lValue(lValue),
	m_dValue(dValue)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(pmdid->FValid());
	
	if (!FNull())
	{
		GPOS_ASSERT(0 < ulSize);

		m_pbVal = GPOS_NEW_ARRAY(m_pmp, BYTE, ulSize);
		(void) clib::PvMemCpy(m_pbVal, pv, ulSize);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::~CDatumGenericGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDatumGenericGPDB::~CDatumGenericGPDB()
{
	GPOS_DELETE_ARRAY(m_pbVal);
	m_pmdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FNull
//
//	@doc:
//		Accessor of is null
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FNull() const
{
	return m_fNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::UlSize
//
//	@doc:
//		Accessor of size
//
//---------------------------------------------------------------------------
ULONG
CDatumGenericGPDB::UlSize() const
{
	return m_ulSize;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::Pmdid
//
//	@doc:
//		Accessor of the type information
//
//---------------------------------------------------------------------------
IMDId *
CDatumGenericGPDB::Pmdid() const
{
	return m_pmdid;
}


INT
CDatumGenericGPDB::ITypeModifier() const
{
	return m_iTypeModifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDatumGenericGPDB::UlHash() const
{
	ULONG ulHash = 0;
	if (FNull())
	{
		ulHash = gpos::UlHash<ULONG>(&ulHash);
	}
	else
	{
		ulHash = gpos::UlHash<BYTE>(&m_pbVal[0]);
		ULONG ulSize = UlSize();
		for (ULONG i = 1; i < ulSize; i++)
		{
			ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHash<BYTE>(&m_pbVal[i]));
		}
	}

	return gpos::UlCombineHashes (m_pmdid->UlHash(), ulHash);
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::Pstr
//
//	@doc:
//		Return string representation
//
//---------------------------------------------------------------------------
const CWStringConst *
CDatumGenericGPDB::Pstr
	(
	IMemoryPool *pmp
	)
	const
{
	CWStringDynamic str(pmp);

	if (FNull())
	{
		str.AppendFormat(GPOS_WSZ_LIT("null"));
		return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
	}

	// pretty print datums that can be mapped to LINTs or CDoubles
	if (FHasStatsLINTMapping())
	{
		str.AppendFormat(GPOS_WSZ_LIT("%0.3f"), (double) LStatsMapping());
		return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
	}
	else if (FHasStatsDoubleMapping())
	{
		str.AppendFormat(GPOS_WSZ_LIT("%0.3f"), DStatsMapping().DVal());
		return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
	}

	// print hex representation of bytes
	ULONG ulSize = UlSize();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		str.AppendFormat(GPOS_WSZ_LIT("%02X"), m_pbVal[ul]);
	}

	return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FMatch
//
//	@doc:
//		Matches the values of datums
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FMatch
	(
	const IDatum *pdatum
	)
	const
{
	if(!pdatum->Pmdid()->FEquals(m_pmdid) || (pdatum->UlSize() != UlSize()))
	{
		return false;
	}

	const CDatumGenericGPDB *pdatumgeneric = dynamic_cast<const CDatumGenericGPDB *>(pdatum);

	if (pdatumgeneric->FNull() && FNull())
	{
		return true;
	}

	if (!pdatumgeneric->FNull() && !FNull())
	{
		if (0 == clib::IMemCmp(pdatumgeneric->m_pbVal, m_pbVal, UlSize()))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::PdatumCopy
//
//	@doc:
//		Returns a copy of the datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumGenericGPDB::PdatumCopy
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();
	
	// CDatumGenericGPDB makes a copy of the buffer
	return GPOS_NEW(pmp) CDatumGenericGPDB(pmp, m_pmdid, m_iTypeModifier, m_pbVal, m_ulSize, m_fNull, m_lValue, m_dValue);
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDatumGenericGPDB::OsPrint
	(
	IOstream &os
	)
	const
{
	const CWStringConst *pstr = Pstr(m_pmp);
	os << pstr->Wsz();
	GPOS_DELETE(pstr);

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FHasStatsDoubleMapping
//
//	@doc:
//		For statistics computation, can this datum be mapped to a CDouble
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FHasStatsDoubleMapping() const
{
	return CMDTypeGenericGPDB::FHasByteDoubleMapping(this->Pmdid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FHasStatsLINTMapping
//
//	@doc:
//		For statistics computation, can this datum be mapped to a LINT
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FHasStatsLINTMapping() const
{
	return CMDTypeGenericGPDB::FHasByteLintMapping(this->Pmdid());

}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FSupportsBinaryComp
//
//	@doc:
//		For statistics computation, can we compare byte array
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FSupportsBinaryComp
	(
	const IDatum *pdatumOther
	)
	const
{
	return ((Pmdid()->FEquals(&CMDIdGPDB::m_mdidBPChar)
			|| Pmdid()->FEquals(&CMDIdGPDB::m_mdidVarChar)
			|| Pmdid()->FEquals(&CMDIdGPDB::m_mdidText))
			&& (this->Pmdid()->Sysid().FEquals(pdatumOther->Pmdid()->Sysid())));
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::PbaVal
//
//	@doc:
//		For statistics computation, return the byte array representation of
//		the datum
//---------------------------------------------------------------------------
const BYTE*
CDatumGenericGPDB::PbaVal
	()
	const
{
	return m_pbVal;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FStatsEqual
//
//	@doc:
//		Are datums statistically equal?
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FStatsEqual
	(
	const IDatum *pdatum
	)
	const
{
	// if mapping exists, use that to compute equality
	if (FHasStatsLINTMapping()
			|| FHasStatsDoubleMapping())
	{
		return IDatumStatisticsMappable::FStatsEqual(pdatum);
	}

	// take special care of nulls
	if (FNull() || pdatum->FNull())
	{
		return FNull() && pdatum->FNull();
	}

	// fall back to memcmp
	const CDatumGenericGPDB *pdatumgenericgpdb
				= dynamic_cast<const CDatumGenericGPDB *> (pdatum);

	ULONG ulSize = this->UlSize();
	if (ulSize == pdatumgenericgpdb->UlSize())
	{
		const BYTE *pb1 = m_pbVal;
		const BYTE *pb2 = pdatumgenericgpdb->m_pbVal;
		return (clib::IMemCmp(pb1, pb2, ulSize) == 0);
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::PbaVal
//
//	@doc:
//		Accessor of byte array
//
//---------------------------------------------------------------------------
BYTE *
CDatumGenericGPDB::PbaVal
	(
	IMemoryPool *pmp,
	ULONG *pulLength
	)
	const
{
	ULONG ulLength = 0;
	BYTE *pba = NULL;

	if (!FNull())
	{
		ulLength = this->UlSize();;
		GPOS_ASSERT(ulLength > 0);
		pba = GPOS_NEW_ARRAY(pmp, BYTE, ulLength);
		(void) clib::PvMemCpy(pba, this->m_pbVal, ulLength);
	}

	*pulLength = ulLength;
	return pba;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FNeedsPadding
//
//	@doc:
//		Does the datum need to be padded before statistical derivation
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FNeedsPadding() const
{
	return Pmdid()->FEquals(&CMDIdGPDB::m_mdidBPChar);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::PdatumPadded
//
//	@doc:
//		Return the padded datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumGenericGPDB::PdatumPadded
	(
	IMemoryPool *pmp,
	ULONG ulColLen
	)
	const
{
	// in GPDB the first four bytes of the datum are used for the header
	const ULONG ulAdjustedColWidth = ulColLen + GPDB_DATUM_HDRSZ;

	if (this->FNull() || (gpos::ulong_max == ulColLen))
	{
		return this->PdatumCopy(pmp);
	}

	const ULONG ulDatumLen = this->UlSize();
	if (gpos::ulong_max != ulAdjustedColWidth &&
		ulDatumLen < ulAdjustedColWidth)
	{
		const BYTE *pbaOriginal = this->PbaVal();
		BYTE *pba = NULL;

		pba = GPOS_NEW_ARRAY(m_pmp, BYTE, ulAdjustedColWidth);
		(void) clib::PvMemCpy(pba, pbaOriginal, ulDatumLen);

		// datum's length smaller than column's size, therefore pad the input datum
		(void) clib::PvMemSet(pba + ulDatumLen, ' ', ulAdjustedColWidth - ulDatumLen);

		// create a new datum
		this->Pmdid()->AddRef();
		CDatumGenericGPDB *pdatumNew = GPOS_NEW(m_pmp) CDatumGenericGPDB
													(
													pmp,
													this->Pmdid(),
													this->ITypeModifier(),
													pba,
													ulAdjustedColWidth,
													this->FNull(),
													this->LStatsMapping(),
													0 /* dValue */
													);

		// clean up the input byte array as the constructor creates a copy
		GPOS_DELETE_ARRAY(pba);

		return pdatumNew;
	}

	return this->PdatumCopy(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::DLikePredicateScaleFactor
//
//	@doc:
//		Return the scale factor of the like predicate by checking the pattern
//		that is being matched in the LIKE predicate
//---------------------------------------------------------------------------
CDouble
CDatumGenericGPDB::DLikePredicateScaleFactor() const
{
	if (this->FNull())
	{
		return CDouble(1.0);
	}

	const ULONG ulDatumLen = this->UlSize();
	const BYTE *pba = this->PbaVal();

	ULONG ulPos = 0;

	// skip any leading %; it's already factored into initial selectivity (DDefaultScaleFactorLike).
	// In GPDB the first four bytes of the datum are used for the header
	for (ulPos = GPDB_DATUM_HDRSZ; ulPos < ulDatumLen; ulPos++)
	{
		if ('%' != pba[ulPos]  && '_' != pba[ulPos])
		{
			break;
		}
	}

	CDouble dSelectivity(1.0);
	CDouble dFixedCharSelectivity = CDatumGenericGPDB::DDefaultFixedCharSelectivity;
	while (ulPos < ulDatumLen)
	{
		// % and _ are wildcard characters in LIKE
		if ('_' == pba[ulPos])
		{
			dSelectivity = dSelectivity * CDatumGenericGPDB::DDefaultAnyCharSelectivity;
		}
		else if ('%' != pba[ulPos])
	    {
			if ('\\' == pba[ulPos])
			{
				// backslash quotes the next character
				ulPos++;
				if (ulPos >= ulDatumLen)
				{
					break;
			    }
			}

			dSelectivity = dSelectivity * dFixedCharSelectivity;
			dFixedCharSelectivity = dFixedCharSelectivity +
									(1.0 - dFixedCharSelectivity) * CDatumGenericGPDB::DDefaultCdbRolloffSelectivity;
		}

		ulPos++;
	}

	dSelectivity = dSelectivity * DTrailingWildcardSelectivity(pba, ulPos);

	return 1 / std::max(dSelectivity, 1/CScaleFactorUtils::DDefaultScaleFactorLike);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::DTrailingWildcardSelectivity
//
//	@doc:
//		Return the selectivity of the trailing wildcards
//
//---------------------------------------------------------------------------
CDouble
CDatumGenericGPDB::DTrailingWildcardSelectivity
	(
	const BYTE *pba,
	ULONG ulPos
	)
	const
{
	GPOS_ASSERT(NULL != pba);

	// If no trailing wildcard, reduce selectivity
	BOOL fWildcard = (0 < ulPos) && ('%' != pba[ulPos-1]);
	BOOL fBackslash = (2 <= ulPos) && ('\\' == pba[ulPos-2]);
	if (fWildcard || fBackslash)
	{
		return CDatumGenericGPDB::DDefaultCdbRanchorSelectivity;
	}

	return CDouble(1.0);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FStatsEqualBinary
//
//	@doc:
//		Equality based on byte array
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FStatsEqualBinary
	(
	const IDatum *pdatumOther
	)
	const
{
	GPOS_ASSERT(NULL != pdatumOther);
	GPOS_ASSERT(this->FSupportsBinaryComp(pdatumOther) && pdatumOther->FSupportsBinaryComp(this));

	// ensure that both datums are from the same system
	// for instance, disallow comparison between GPDB char and HD char
	GPOS_ASSERT(this->Pmdid()->Sysid().FEquals(pdatumOther->Pmdid()->Sysid()));

	if (FNull() || pdatumOther->FNull())
	{
		return false;
	}

	const ULONG ulLen = this->UlSize();
	const ULONG ulLenOther = pdatumOther->UlSize();

	if (ulLen != ulLenOther)
	{
		return false;
	}

	// compare the two BYTEA after offsetting used by the GPDB datum header length
	const BYTE *pba = this->PbaVal();
	const BYTE *pbaOther = pdatumOther->PbaVal();
	INT iResult = gpos::clib::IMemCmp
								(
								pba + GPDB_DATUM_HDRSZ,
								pbaOther + GPDB_DATUM_HDRSZ,
								ulLen - GPDB_DATUM_HDRSZ
								);

	return (0 == iResult);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumGenericGPDB::FStatsLessThanBinary
//
//	@doc:
//		Less-than comparison based on byte array
//
//---------------------------------------------------------------------------
BOOL
CDatumGenericGPDB::FStatsLessThanBinary
	(
	const IDatum *pdatumOther
	)
	const
{
	GPOS_ASSERT(NULL != pdatumOther);
	GPOS_ASSERT(this->FSupportsBinaryComp(pdatumOther) && pdatumOther->FSupportsBinaryComp(this));

	// ensure that both datums are from the same system
	// for instance, disallow comparison between GPDB char and HD char
	GPOS_ASSERT(this->Pmdid()->Sysid().FEquals(pdatumOther->Pmdid()->Sysid()));

	if (FNull() || pdatumOther->FNull())
	{
		return false;
	}

	const ULONG ulLenOther = pdatumOther->UlSize();

	ULONG ulLenComparison = this->UlSize();
	if (this->UlSize() > ulLenOther)
	{
		ulLenComparison = ulLenOther;
	}

	const BYTE *pba = this->PbaVal();
	const BYTE *pbaOther = pdatumOther->PbaVal();

	// compare the two BYTEA after offset-ing used by the GPDB datum header length
	INT iResult = gpos::clib::IMemCmp
								(
								pba + GPDB_DATUM_HDRSZ,
								pbaOther + GPDB_DATUM_HDRSZ,
								ulLenComparison - GPDB_DATUM_HDRSZ
								);

	if (0 > iResult)
	{
		return true;
	}

	return (0 == iResult && this->UlSize() < ulLenOther);
}

// EOF

