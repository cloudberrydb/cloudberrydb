/*
 * xml.h
 *
 * This file is based on src/include/utils/xml.h from the PostgreSQL 9.1 
 * distribution whose original header is below.	 The primary differences
 * between this code and the original are as follows:
 * 
 * 1. The prototypes for the following functions we do not support were removed:
 *		table_to_xml
 *		query_to_xml
 *		cursor_to_xml
 *		table_to_xmlschema
 *		query_to_xmlschema
 *		cursor_to_xmlschema
 *		table_to_xml_and_xmlschema
 *		query_to_xml_and_xmlschema
 *		schema_to_xml
 *		schema_to_xmlschema
 *		schema_to_xml_and_xmlschema
 *		database_to_xml
 *		database_to_xmlschema
 *		database_to_xml_and_xmlschema
 *		map_sql_identifier_to_xml_name
 *		map_xml_name_to_sql_identifier
 *      xmlelement
 *
 * 2. The prototypes for functions we needed for GBDB were added:
 *      xmleq
 *      xmlne
 *
 * 3. XmlOptionType was added here from src/include/nodes/primnodes.h
 *
 */

/*-------------------------------------------------------------------------
 *
 * xml.h
 *	  Declarations for XML data type support.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/xml.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef XML_H
#define XML_H

#include "fmgr.h"
#include "nodes/execnodes.h"
#include "nodes/primnodes.h"

typedef enum
{
	XMLOPTION_DOCUMENT,
	XMLOPTION_CONTENT
} XmlOptionType;

typedef struct varlena xmltype;

#define DatumGetXmlP(X)		((xmltype *) PG_DETOAST_DATUM(X))
#define XmlPGetDatum(X)		PointerGetDatum(X)

#define PG_GETARG_XML_P(n)	DatumGetXmlP(PG_GETARG_DATUM(n))
#define PG_RETURN_XML_P(x)	PG_RETURN_POINTER(x)

extern Datum xml_in(PG_FUNCTION_ARGS);
extern Datum xml_out(PG_FUNCTION_ARGS);
extern Datum xml_recv(PG_FUNCTION_ARGS);
extern Datum xml_send(PG_FUNCTION_ARGS);
extern Datum xmlcomment(PG_FUNCTION_ARGS);
extern Datum xmlconcat2(PG_FUNCTION_ARGS);
extern Datum texttoxml(PG_FUNCTION_ARGS);
extern Datum xmltotext(PG_FUNCTION_ARGS);
extern Datum xmlvalidate(PG_FUNCTION_ARGS);
extern Datum xpath(PG_FUNCTION_ARGS);
extern Datum xpath_exists(PG_FUNCTION_ARGS);
extern Datum xmlexists(PG_FUNCTION_ARGS);
extern Datum xml_is_well_formed(PG_FUNCTION_ARGS);
extern Datum xml_is_well_formed_document(PG_FUNCTION_ARGS);
extern Datum xml_is_well_formed_content(PG_FUNCTION_ARGS);

typedef enum
{
	XML_STANDALONE_YES,
	XML_STANDALONE_NO,
	XML_STANDALONE_NO_VALUE,
	XML_STANDALONE_OMITTED
}	XmlStandaloneType;

extern void pg_xml_init(void);
extern void xml_ereport(int level, int sqlcode, const char *msg);
extern xmltype *xmlconcat(List *args);
extern xmltype *xmlparse(text *data, XmlOptionType xmloption, bool preserve_whitespace);
extern xmltype *xmlpi(char *target, text *arg, bool arg_is_null, bool *result_is_null);
extern xmltype *xmlroot(xmltype *data, text *version, int standalone);
extern bool xml_is_document(xmltype *arg);
extern text *xmltotext_with_xmloption(xmltype *data, XmlOptionType xmloption_arg);
extern char *escape_xml(const char *str);

typedef enum
{
	XMLBINARY_BASE64,
	XMLBINARY_HEX
}	XmlBinaryType;

extern int	xmlbinary;			/* XmlBinaryType, but int for guc enum */

extern int	xmloption;			/* XmlOptionType, but int for guc enum */

#endif   /* XML_H */
