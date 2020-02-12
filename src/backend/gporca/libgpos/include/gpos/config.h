//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Greenplum, Inc.
//
//	@filename:
//		config.h
//
//	@doc:
//		Various compile-time options that affect binary
//		compatibility.
//
//---------------------------------------------------------------------------
#ifndef GPOS_config_H
#define GPOS_config_H

/* Get this working for now. */
/* Idealy we should generate this files just like pg_config.h is generated */

#ifdef USE_ASSERT_CHECKING
#define GPOS_DEBUG
#else
#undef GPOS_DEBUG
#endif

#define GPOS_Darwin

#endif  // GPOS_config_H
// EOF
