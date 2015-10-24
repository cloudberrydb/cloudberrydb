/*
 * xml.c
 *
 * This file is based on backend/utils/adt/xml.c from the PostgreSQL 9.1 
 * distribution whose original header is below.  The primary differences
 * between this code and the original are as follows:
 * 
 * 1. As use of libxml is required, conditional #ifdef codepaths were removed.
 *
 * 2. Since these were not needed for XPath support, the following functions
 *    and macros (mainly for SQL/XML:2008) were removed.
 *    
 *    NAMESPACE_SQLXML
 *    NAMESPACE_XSD
 *    NAMESPACE_XSI
 *    NO_XML_SUPPORT
 *    SPI_sql_row_to_xmlelement
 *    XML_VISIBLE_SCHEMAS
 *    XML_VISIBLE_SCHEMAS_EXCLUDE
 *    _SPI_strdup
 *    cstring_to_xmltype
 *    cursor_to_xml
 *    cursor_to_xmlschema
 *    database_get_xml_visible_schemas
 *    database_get_xml_visible_tables
 *    database_to_xml
 *    database_to_xml_and_xmlschema
 *    database_to_xml_internal
 *    database_to_xmlschema
 *    database_to_xmlschema_internal
 *    escape_xml
 *    is_valid_xml_namechar
 *    is_valid_xml_namefirst
 *    map_multipart_sql_identifier_to_xml_name
 *    map_sql_catalog_to_xmlschema_types
 *    map_sql_schema_to_xmlschema_types
 *    map_sql_table_to_xmlschema
 *    map_sql_type_to_xml_name
 *    map_sql_type_to_xmlschema_type
 *    map_sql_typecoll_to_xmlschema_types
 *    map_sql_value_to_xml_value
 *    map_xml_name_to_sql_identifier
 *    query_to_oid_list
 *    query_to_xml
 *    query_to_xml_and_xmlschema
 *    query_to_xml_internal
 *    query_to_xmlschema
 *    schema_get_xml_visible_tables
 *    schema_to_xml
 *    schema_to_xml_and_xmlschema
 *    schema_to_xml_internal
 *    schema_to_xmlschema
 *    schema_to_xmlschema_internal
 *    sqlchar_to_unicode
 *    table_to_xml
 *    table_to_xml_and_xmlschema
 *    table_to_xml_internal
 *    table_to_xmlschema
 *    unicode_to_sqlchar
 *    xmldata_root_element_end
 *    xmldata_root_element_start
 *    xmlelement
 *    xsd_schema_element_end
 *    xsd_schema_element_start 
 *    
 * 3. the xpath() function was changed to call
 *		PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
 *    instead of 
 * 		PG_RETURN_ARRAYTYPE_P(makeArrayResult(astate, CurrentMemoryContext));
 *    since the correct return type of makeArrayResult() is Datum.
 *
 * 4. due to a 'xml.c:1085: error: 'count' may be used uninitialized in this function'
 *    compiler warning, 
 *			size_t		count;
 *    was changed to 
 *			size_t		count = 0;
 */

/*-------------------------------------------------------------------------
 *
 * xml.c
 *	  XML data type support.
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/utils/adt/xml.c
 *
 *-------------------------------------------------------------------------
 */

/*
 * Generally, XML type support is only available when libxml use was
 * configured during the build.  But even if that is not done, the
 * type and all the functions are available, but most of them will
 * fail.  For one thing, this avoids having to manage variant catalog
 * installations.  But it also has nice effects such as that you can
 * dump a database containing XML type data even if the server is not
 * linked with libxml.	Thus, make sure xml_out() works even if nothing
 * else does.
 */

/*
 * Notes on memory management:
 *
 * Sometimes libxml allocates global structures in the hope that it can reuse
 * them later on.  This makes it impractical to change the xmlMemSetup
 * functions on-the-fly; that is likely to lead to trying to pfree() chunks
 * allocated with malloc() or vice versa.  Since libxml might be used by
 * loadable modules, eg libperl, our only safe choices are to change the
 * functions at postmaster/backend launch or not at all.  Since we'd rather
 * not activate libxml in sessions that might never use it, the latter choice
 * is the preferred one.  However, for debugging purposes it can be awfully
 * handy to constrain libxml's allocations to be done in a specific palloc
 * context, where they're easy to track.  Therefore there is code here that
 * can be enabled in debug builds to redirect libxml's allocations into a
 * special context LibxmlContext.  It's not recommended to turn this on in
 * a production build because of the possibility of bad interactions with
 * external modules.
 */
/* #define USE_LIBXMLCONTEXT */

#include "postgres.h"

#ifdef USE_LIBXML
#include <libxml/chvalid.h>
#include <libxml/parser.h>
#include <libxml/parserInternals.h>
#include <libxml/tree.h>
#include <libxml/uri.h>
#include <libxml/xmlerror.h>
#include <libxml/xmlwriter.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>
#endif	/* USE_LIBXML */

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/xml.h"


/* GUC variables */
int			xmlbinary;
int			xmloption;

#ifdef USE_LIBXML

static StringInfo xml_err_buf = NULL;

static xmlParserInputPtr xmlPgEntityLoader(const char *URL, const char *ID,
				  xmlParserCtxtPtr ctxt);
static void xml_errorHandler(void *ctxt, const char *msg,...);
static void xml_ereport_by_code(int level, int sqlcode,
					const char *msg, int errcode);

#ifdef USE_LIBXMLCONTEXT

static MemoryContext LibxmlContext = NULL;

static void xml_memory_init(void);
static void *xml_palloc(size_t size);
static void *xml_repalloc(void *ptr, size_t size);
static void xml_pfree(void *ptr);
static char *xml_pstrdup(const char *string);
#endif   /* USE_LIBXMLCONTEXT */

static xmlChar *xml_text2xmlChar(text *in);
static int parse_xml_decl(const xmlChar *str, size_t *lenp,
			   xmlChar **version, xmlChar **encoding, int *standalone);
static bool print_xml_decl(StringInfo buf, const xmlChar *version,
			   pg_enc encoding, int standalone);
static xmlDocPtr xml_parse(text *data, XmlOptionType xmloption_arg,
		  bool preserve_whitespace, int encoding);
static text *xml_xmlnodetoxmltype(xmlNodePtr cur);
#endif	/* USE_LIBXML */

#define NO_XML_SUPPORT() \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("unsupported XML feature"), \
			 errdetail("This functionality requires the server to be built with libxml support."), \
			 errhint("You need to rebuild PostgreSQL using --with-libxml.")))


#ifdef USE_LIBXML

static int
xmlChar_to_encoding(const xmlChar *encoding_name)
{
	int			encoding = pg_char_to_encoding((const char *) encoding_name);

	if (encoding < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid encoding name \"%s\"",
						(const char *) encoding_name)));
	return encoding;
}
#endif


/*
 * xml_in uses a plain C string to VARDATA conversion, so for the time being
 * we use the conversion function for the text datatype.
 *
 * This is only acceptable so long as xmltype and text use the same
 * representation.
 */
Datum
xml_in(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	char	   *s = PG_GETARG_CSTRING(0);
	xmltype    *vardata;
	xmlDocPtr	doc;

	vardata = (xmltype *) cstring_to_text(s);

	/*
	 * Parse the data to check if it is well-formed XML data.  Assume that
	 * ERROR occurred if parsing failed.
	 */
	doc = xml_parse(vardata, xmloption, true, GetDatabaseEncoding());
	xmlFreeDoc(doc);

	PG_RETURN_XML_P(vardata);
#else
	NO_XML_SUPPORT();
	return 0;
#endif
}


#define PG_XML_DEFAULT_VERSION "1.0"


/*
 * xml_out_internal uses a plain VARDATA to C string conversion, so for the
 * time being we use the conversion function for the text datatype.
 *
 * This is only acceptable so long as xmltype and text use the same
 * representation.
 */
static char *
xml_out_internal(xmltype *x, pg_enc target_encoding)
{
	char	   *str = text_to_cstring((text *) x);

#ifdef USE_LIBXML
	size_t		len = strlen(str);
	xmlChar    *version;
	int			standalone;
	int			res_code;

	if ((res_code = parse_xml_decl((xmlChar *) str,
								   &len, &version, NULL, &standalone)) == 0)
	{
		StringInfoData buf;

		initStringInfo(&buf);

		if (!print_xml_decl(&buf, version, target_encoding, standalone))
		{
			/*
			 * If we are not going to produce an XML declaration, eat a single
			 * newline in the original string to prevent empty first lines in
			 * the output.
			 */
			if (*(str + len) == '\n')
				len += 1;
		}
		appendStringInfoString(&buf, str + len);

		pfree(str);

		return buf.data;
	}

	xml_ereport_by_code(WARNING, ERRCODE_INTERNAL_ERROR,
						"could not parse XML declaration in stored value",
						res_code);
#endif
	return str;
}


Datum
xml_out(PG_FUNCTION_ARGS)
{
	xmltype    *x = PG_GETARG_XML_P(0);

	/*
	 * xml_out removes the encoding property in all cases.	This is because we
	 * cannot control from here whether the datum will be converted to a
	 * different client encoding, so we'd do more harm than good by including
	 * it.
	 */
	PG_RETURN_CSTRING(xml_out_internal(x, 0));
}


Datum
xml_recv(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	xmltype    *result;
	char	   *str;
	char	   *newstr;
	int			nbytes;
	xmlDocPtr	doc;
	xmlChar    *encodingStr = NULL;
	int			encoding;

	/*
	 * Read the data in raw format. We don't know yet what the encoding is, as
	 * that information is embedded in the xml declaration; so we have to
	 * parse that before converting to server encoding.
	 */
	nbytes = buf->len - buf->cursor;
	str = (char *) pq_getmsgbytes(buf, nbytes);

	/*
	 * We need a null-terminated string to pass to parse_xml_decl().  Rather
	 * than make a separate copy, make the temporary result one byte bigger
	 * than it needs to be.
	 */
	result = palloc(nbytes + 1 + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);
	memcpy(VARDATA(result), str, nbytes);
	str = VARDATA(result);
	str[nbytes] = '\0';

	parse_xml_decl((xmlChar *) str, NULL, NULL, &encodingStr, NULL);

	/*
	 * If encoding wasn't explicitly specified in the XML header, treat it as
	 * UTF-8, as that's the default in XML. This is different from xml_in(),
	 * where the input has to go through the normal client to server encoding
	 * conversion.
	 */
	encoding = encodingStr ? xmlChar_to_encoding(encodingStr) : PG_UTF8;

	/*
	 * Parse the data to check if it is well-formed XML data.  Assume that
	 * xml_parse will throw ERROR if not.
	 */
	doc = xml_parse(result, xmloption, true, encoding);
	xmlFreeDoc(doc);

	/* Now that we know what we're dealing with, convert to server encoding */
	newstr = (char *) pg_do_encoding_conversion((unsigned char *) str,
												nbytes,
												encoding,
												GetDatabaseEncoding());

	if (newstr != str)
	{
		pfree(result);
		result = (xmltype *) cstring_to_text(newstr);
		pfree(newstr);
	}

	PG_RETURN_XML_P(result);
#else
	NO_XML_SUPPORT();
	return 0;
#endif
}


Datum
xml_send(PG_FUNCTION_ARGS)
{
	xmltype    *x = PG_GETARG_XML_P(0);
	char	   *outval;
	StringInfoData buf;

	/*
	 * xml_out_internal doesn't convert the encoding, it just prints the right
	 * declaration. pq_sendtext will do the conversion.
	 */
	outval = xml_out_internal(x, pg_get_client_encoding());

	pq_begintypsend(&buf);
	pq_sendtext(&buf, outval, strlen(outval));
	pfree(outval);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


static void
appendStringInfoText(StringInfo str, const text *t)
{
	appendBinaryStringInfo(str, VARDATA(t), VARSIZE(t) - VARHDRSZ);
}


static xmltype *
stringinfo_to_xmltype(StringInfo buf)
{
	return (xmltype *) cstring_to_text_with_len(buf->data, buf->len);
}


#ifdef USE_LIBXML
static xmltype *
xmlBuffer_to_xmltype(xmlBufferPtr buf)
{
	return (xmltype *) cstring_to_text_with_len((char *) xmlBufferContent(buf),
												xmlBufferLength(buf));
}
#endif


Datum
xmlcomment(PG_FUNCTION_ARGS)
{
	text	   *arg = PG_GETARG_TEXT_P(0);
	char	   *argdata = VARDATA(arg);
	int			len = VARSIZE(arg) - VARHDRSZ;
	StringInfoData buf;
	int			i;

	/* check for "--" in string or "-" at the end */
	for (i = 1; i < len; i++)
	{
		if (argdata[i] == '-' && argdata[i - 1] == '-')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_XML_COMMENT),
					 errmsg("invalid XML comment")));
	}
	if (len > 0 && argdata[len - 1] == '-')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_XML_COMMENT),
				 errmsg("invalid XML comment")));

	initStringInfo(&buf);
	appendStringInfo(&buf, "<!--");
	appendStringInfoText(&buf, arg);
	appendStringInfo(&buf, "-->");

	PG_RETURN_XML_P(stringinfo_to_xmltype(&buf));
}



/*
 * TODO: xmlconcat needs to merge the notations and unparsed entities
 * of the argument values.	Not very important in practice, though.
 */
xmltype *
xmlconcat(List *args)
{
#ifdef USE_LIBXML
	int			global_standalone = 1;
	xmlChar    *global_version = NULL;
	bool		global_version_no_value = false;
	StringInfoData buf;
	ListCell   *v;

	initStringInfo(&buf);
	foreach(v, args)
	{
		xmltype    *x = DatumGetXmlP(PointerGetDatum(lfirst(v)));
		size_t		len;
		xmlChar    *version;
		int			standalone;
		char	   *str;

		len = VARSIZE(x) - VARHDRSZ;
		str = text_to_cstring((text *) x);

		parse_xml_decl((xmlChar *) str, &len, &version, NULL, &standalone);

		if (standalone == 0 && global_standalone == 1)
			global_standalone = 0;
		if (standalone < 0)
			global_standalone = -1;

		if (!version)
			global_version_no_value = true;
		else if (!global_version)
			global_version = version;
		else if (xmlStrcmp(version, global_version) != 0)
			global_version_no_value = true;

		appendStringInfoString(&buf, str + len);
		pfree(str);
	}

	if (!global_version_no_value || global_standalone >= 0)
	{
		StringInfoData buf2;

		initStringInfo(&buf2);

		print_xml_decl(&buf2,
					   (!global_version_no_value) ? global_version : NULL,
					   0,
					   global_standalone);

		appendStringInfoString(&buf2, buf.data);
		buf = buf2;
	}

	return stringinfo_to_xmltype(&buf);
#else
	NO_XML_SUPPORT();
	return NULL;
#endif
}


/*
 * XMLAGG support
 */
Datum
xmlconcat2(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		if (PG_ARGISNULL(1))
			PG_RETURN_NULL();
		else
			PG_RETURN_XML_P(PG_GETARG_XML_P(1));
	}
	else if (PG_ARGISNULL(1))
		PG_RETURN_XML_P(PG_GETARG_XML_P(0));
	else
		PG_RETURN_XML_P(xmlconcat(list_make2(PG_GETARG_XML_P(0),
											 PG_GETARG_XML_P(1))));
}


Datum
texttoxml(PG_FUNCTION_ARGS)
{
	text	   *data = PG_GETARG_TEXT_P(0);

	PG_RETURN_XML_P(xmlparse(data, xmloption, true));
}


Datum
xmltotext(PG_FUNCTION_ARGS)
{
	xmltype    *data = PG_GETARG_XML_P(0);

	/* It's actually binary compatible. */
	PG_RETURN_TEXT_P((text *) data);
}


text *
xmltotext_with_xmloption(xmltype *data, XmlOptionType xmloption_arg)
{
	if (xmloption_arg == XMLOPTION_DOCUMENT && !xml_is_document(data))
		ereport(ERROR,
				(errcode(ERRCODE_NOT_AN_XML_DOCUMENT),
				 errmsg("not an XML document")));

	/* It's actually binary compatible, save for the above check. */
	return (text *) data;
}



xmltype *
xmlparse(text *data, XmlOptionType xmloption_arg, bool preserve_whitespace)
{
#ifdef USE_LIBXML
	xmlDocPtr	doc;

	doc = xml_parse(data, xmloption_arg, preserve_whitespace,
					GetDatabaseEncoding());
	xmlFreeDoc(doc);

	return (xmltype *) data;
#else
	NO_XML_SUPPORT();
	return NULL;
#endif
}


xmltype *
xmlpi(char *target, text *arg, bool arg_is_null, bool *result_is_null)
{
	xmltype    *result;
	StringInfoData buf;

	if (pg_strcasecmp(target, "xml") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR), /* really */
				 errmsg("invalid XML processing instruction"),
				 errdetail("XML processing instruction target name cannot be \"%s\".", target)));

	/*
	 * Following the SQL standard, the null check comes after the syntax check
	 * above.
	 */
	*result_is_null = arg_is_null;
	if (*result_is_null)
		return NULL;

	initStringInfo(&buf);

	appendStringInfo(&buf, "<?%s", target);

	if (arg != NULL)
	{
		char	   *string;

		string = text_to_cstring(arg);
		if (strstr(string, "?>") != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_XML_PROCESSING_INSTRUCTION),
					 errmsg("invalid XML processing instruction"),
			errdetail("XML processing instruction cannot contain \"?>\".")));

		appendStringInfoChar(&buf, ' ');
		appendStringInfoString(&buf, string + strspn(string, " "));
		pfree(string);
	}
	appendStringInfoString(&buf, "?>");

	result = stringinfo_to_xmltype(&buf);
	pfree(buf.data);
	return result;
}


xmltype *
xmlroot(xmltype *data, text *version, int standalone)
{
#ifdef USE_LIBXML
	char	   *str;
	size_t		len;
	xmlChar    *orig_version;
	int			orig_standalone;
	StringInfoData buf;

	len = VARSIZE(data) - VARHDRSZ;
	str = text_to_cstring((text *) data);

	parse_xml_decl((xmlChar *) str, &len, &orig_version, NULL, &orig_standalone);

	if (version)
		orig_version = xml_text2xmlChar(version);
	else
		orig_version = NULL;

	switch (standalone)
	{
		case XML_STANDALONE_YES:
			orig_standalone = 1;
			break;
		case XML_STANDALONE_NO:
			orig_standalone = 0;
			break;
		case XML_STANDALONE_NO_VALUE:
			orig_standalone = -1;
			break;
		case XML_STANDALONE_OMITTED:
			/* leave original value */
			break;
	}

	initStringInfo(&buf);
	print_xml_decl(&buf, orig_version, 0, orig_standalone);
	appendStringInfoString(&buf, str + len);

	return stringinfo_to_xmltype(&buf);
#else
	NO_XML_SUPPORT();
	return NULL;
#endif
}


/*
 * Validate document (given as string) against DTD (given as external link)
 *
 * This has been removed because it is a security hole: unprivileged users
 * should not be able to use Postgres to fetch arbitrary external files,
 * which unfortunately is exactly what libxml is willing to do with the DTD
 * parameter.
 */
Datum
xmlvalidate(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("xmlvalidate is not implemented")));
	return 0;
}


bool
xml_is_document(xmltype *arg)
{
#ifdef USE_LIBXML
	bool		result;
	xmlDocPtr	doc = NULL;
	MemoryContext ccxt = CurrentMemoryContext;

	/* We want to catch ereport(INVALID_XML_DOCUMENT) and return false */
	PG_TRY();
	{
		doc = xml_parse((text *) arg, XMLOPTION_DOCUMENT, true,
						GetDatabaseEncoding());
		result = true;
	}
	PG_CATCH();
	{
		ErrorData  *errdata;
		MemoryContext ecxt;

		ecxt = MemoryContextSwitchTo(ccxt);
		errdata = CopyErrorData();
		if (errdata->sqlerrcode == ERRCODE_INVALID_XML_DOCUMENT)
		{
			FlushErrorState();
			result = false;
		}
		else
		{
			MemoryContextSwitchTo(ecxt);
			PG_RE_THROW();
		}
	}
	PG_END_TRY();

	if (doc)
		xmlFreeDoc(doc);

	return result;
#else							/* not USE_LIBXML */
	NO_XML_SUPPORT();
	return false;
#endif   /* not USE_LIBXML */
}


#ifdef USE_LIBXML

/*
 * pg_xml_init --- set up for use of libxml
 *
 * This should be called by each function that is about to use libxml
 * facilities.	It has two responsibilities: verify compatibility with the
 * loaded libxml version (done on first call in a session) and establish
 * or re-establish our libxml error handler.  The latter needs to be done
 * anytime we might have passed control to add-on modules (eg libperl) which
 * might have set their own error handler for libxml.
 *
 * This is exported for use by contrib/xml2, as well as other code that might
 * wish to share use of this module's libxml error handler.
 *
 * TODO: xmlChar is utf8-char, make proper tuning (initdb with enc!=utf8 and
 * check)
 */
void
pg_xml_init(void)
{
	static bool first_time = true;

	if (first_time)
	{
		/* Stuff we need do only once per session */
		MemoryContext oldcontext;

		/*
		 * Currently, we have no pure UTF-8 support for internals -- check if
		 * we can work.
		 */
		if (sizeof(char) != sizeof(xmlChar))
			ereport(ERROR,
					(errmsg("could not initialize XML library"),
					 errdetail("libxml2 has incompatible char type: sizeof(char)=%u, sizeof(xmlChar)=%u.",
							   (int) sizeof(char), (int) sizeof(xmlChar))));

		/* create error buffer in permanent context */
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		xml_err_buf = makeStringInfo();
		MemoryContextSwitchTo(oldcontext);

		/* Now that xml_err_buf exists, safe to call xml_errorHandler */
		xmlSetGenericErrorFunc(NULL, xml_errorHandler);

		/* set up our entity loader, too */
		xmlSetExternalEntityLoader(xmlPgEntityLoader);

#ifdef USE_LIBXMLCONTEXT
		/* Set up memory allocation our way, too */
		xml_memory_init();
#endif

		/* Check library compatibility */
		LIBXML_TEST_VERSION;

		first_time = false;
	}
	else
	{
		/* Reset pre-existing buffer to empty */
		Assert(xml_err_buf != NULL);
		resetStringInfo(xml_err_buf);

		/*
		 * We re-establish the error callback function every time.	This makes
		 * it safe for other subsystems (PL/Perl, say) to also use libxml with
		 * their own callbacks ... so long as they likewise set up the
		 * callbacks on every use. It's cheap enough to not be worth worrying
		 * about, anyway.
		 */
		xmlSetGenericErrorFunc(NULL, xml_errorHandler);

		/* set up our entity loader, too */
		xmlSetExternalEntityLoader(xmlPgEntityLoader);
	}
}


/*
 * SQL/XML allows storing "XML documents" or "XML content".  "XML
 * documents" are specified by the XML specification and are parsed
 * easily by libxml.  "XML content" is specified by SQL/XML as the
 * production "XMLDecl? content".  But libxml can only parse the
 * "content" part, so we have to parse the XML declaration ourselves
 * to complete this.
 */

#define CHECK_XML_SPACE(p) \
	do { \
		if (!xmlIsBlank_ch(*(p))) \
			return XML_ERR_SPACE_REQUIRED; \
	} while (0)

#define SKIP_XML_SPACE(p) \
	while (xmlIsBlank_ch(*(p))) (p)++

/* Letter | Digit | '.' | '-' | '_' | ':' | CombiningChar | Extender */
/* Beware of multiple evaluations of argument! */
#define PG_XMLISNAMECHAR(c) \
	(xmlIsBaseChar_ch(c) || xmlIsIdeographicQ(c) \
			|| xmlIsDigit_ch(c) \
			|| c == '.' || c == '-' || c == '_' || c == ':' \
			|| xmlIsCombiningQ(c) \
			|| xmlIsExtender_ch(c))

/* pnstrdup, but deal with xmlChar not char; len is measured in xmlChars */
static xmlChar *
xml_pnstrdup(const xmlChar *str, size_t len)
{
	xmlChar    *result;

	result = (xmlChar *) palloc((len + 1) * sizeof(xmlChar));
	memcpy(result, str, len * sizeof(xmlChar));
	result[len] = 0;
	return result;
}

/*
 * str is the null-terminated input string.  Remaining arguments are
 * output arguments; each can be NULL if value is not wanted.
 * version and encoding are returned as locally-palloc'd strings.
 * Result is 0 if OK, an error code if not.
 */
static int
parse_xml_decl(const xmlChar *str, size_t *lenp,
			   xmlChar **version, xmlChar **encoding, int *standalone)
{
	const xmlChar *p;
	const xmlChar *save_p;
	size_t		len;
	int			utf8char;
	int			utf8len;

	pg_xml_init();

	/* Initialize output arguments to "not present" */
	if (version)
		*version = NULL;
	if (encoding)
		*encoding = NULL;
	if (standalone)
		*standalone = -1;

	p = str;

	if (xmlStrncmp(p, (xmlChar *) "<?xml", 5) != 0)
		goto finished;

	/* if next char is name char, it's a PI like <?xml-stylesheet ...?> */
	utf8len = strlen((const char *) (p + 5));
	utf8char = xmlGetUTF8Char(p + 5, &utf8len);
	if (PG_XMLISNAMECHAR(utf8char))
		goto finished;

	p += 5;

	/* version */
	CHECK_XML_SPACE(p);
	SKIP_XML_SPACE(p);
	if (xmlStrncmp(p, (xmlChar *) "version", 7) != 0)
		return XML_ERR_VERSION_MISSING;
	p += 7;
	SKIP_XML_SPACE(p);
	if (*p != '=')
		return XML_ERR_VERSION_MISSING;
	p += 1;
	SKIP_XML_SPACE(p);

	if (*p == '\'' || *p == '"')
	{
		const xmlChar *q;

		q = xmlStrchr(p + 1, *p);
		if (!q)
			return XML_ERR_VERSION_MISSING;

		if (version)
			*version = xml_pnstrdup(p + 1, q - p - 1);
		p = q + 1;
	}
	else
		return XML_ERR_VERSION_MISSING;

	/* encoding */
	save_p = p;
	SKIP_XML_SPACE(p);
	if (xmlStrncmp(p, (xmlChar *) "encoding", 8) == 0)
	{
		CHECK_XML_SPACE(save_p);
		p += 8;
		SKIP_XML_SPACE(p);
		if (*p != '=')
			return XML_ERR_MISSING_ENCODING;
		p += 1;
		SKIP_XML_SPACE(p);

		if (*p == '\'' || *p == '"')
		{
			const xmlChar *q;

			q = xmlStrchr(p + 1, *p);
			if (!q)
				return XML_ERR_MISSING_ENCODING;

			if (encoding)
				*encoding = xml_pnstrdup(p + 1, q - p - 1);
			p = q + 1;
		}
		else
			return XML_ERR_MISSING_ENCODING;
	}
	else
	{
		p = save_p;
	}

	/* standalone */
	save_p = p;
	SKIP_XML_SPACE(p);
	if (xmlStrncmp(p, (xmlChar *) "standalone", 10) == 0)
	{
		CHECK_XML_SPACE(save_p);
		p += 10;
		SKIP_XML_SPACE(p);
		if (*p != '=')
			return XML_ERR_STANDALONE_VALUE;
		p += 1;
		SKIP_XML_SPACE(p);
		if (xmlStrncmp(p, (xmlChar *) "'yes'", 5) == 0 ||
			xmlStrncmp(p, (xmlChar *) "\"yes\"", 5) == 0)
		{
			if (standalone)
				*standalone = 1;
			p += 5;
		}
		else if (xmlStrncmp(p, (xmlChar *) "'no'", 4) == 0 ||
				 xmlStrncmp(p, (xmlChar *) "\"no\"", 4) == 0)
		{
			if (standalone)
				*standalone = 0;
			p += 4;
		}
		else
			return XML_ERR_STANDALONE_VALUE;
	}
	else
	{
		p = save_p;
	}

	SKIP_XML_SPACE(p);
	if (xmlStrncmp(p, (xmlChar *) "?>", 2) != 0)
		return XML_ERR_XMLDECL_NOT_FINISHED;
	p += 2;

finished:
	len = p - str;

	for (p = str; p < str + len; p++)
		if (*p > 127)
			return XML_ERR_INVALID_CHAR;

	if (lenp)
		*lenp = len;

	return XML_ERR_OK;
}


/*
 * Write an XML declaration.  On output, we adjust the XML declaration
 * as follows.	(These rules are the moral equivalent of the clause
 * "Serialization of an XML value" in the SQL standard.)
 *
 * We try to avoid generating an XML declaration if possible.  This is
 * so that you don't get trivial things like xml '<foo/>' resulting in
 * '<?xml version="1.0"?><foo/>', which would surely be annoying.  We
 * must provide a declaration if the standalone property is specified
 * or if we include an encoding declaration.  If we have a
 * declaration, we must specify a version (XML requires this).
 * Otherwise we only make a declaration if the version is not "1.0",
 * which is the default version specified in SQL:2003.
 */
static bool
print_xml_decl(StringInfo buf, const xmlChar *version,
			   pg_enc encoding, int standalone)
{
	pg_xml_init();				/* why is this here? */

	if ((version && strcmp((char *) version, PG_XML_DEFAULT_VERSION) != 0)
		|| (encoding && encoding != PG_UTF8)
		|| standalone != -1)
	{
		appendStringInfoString(buf, "<?xml");

		if (version)
			appendStringInfo(buf, " version=\"%s\"", version);
		else
			appendStringInfo(buf, " version=\"%s\"", PG_XML_DEFAULT_VERSION);

		if (encoding && encoding != PG_UTF8)
		{
			/*
			 * XXX might be useful to convert this to IANA names (ISO-8859-1
			 * instead of LATIN1 etc.); needs field experience
			 */
			appendStringInfo(buf, " encoding=\"%s\"",
							 pg_encoding_to_char(encoding));
		}

		if (standalone == 1)
			appendStringInfoString(buf, " standalone=\"yes\"");
		else if (standalone == 0)
			appendStringInfoString(buf, " standalone=\"no\"");
		appendStringInfoString(buf, "?>");

		return true;
	}
	else
		return false;
}


/*
 * Convert a C string to XML internal representation
 *
 * Note: it is caller's responsibility to xmlFreeDoc() the result,
 * else a permanent memory leak will ensue!
 *
 * TODO maybe libxml2's xmlreader is better? (do not construct DOM,
 * yet do not use SAX - see xmlreader.c)
 */
static xmlDocPtr
xml_parse(text *data, XmlOptionType xmloption_arg, bool preserve_whitespace,
		  int encoding)
{
	int32		len;
	xmlChar    *string;
	xmlChar    *utf8string;
	xmlParserCtxtPtr ctxt;
	xmlDocPtr	doc;

	len = VARSIZE(data) - VARHDRSZ;		/* will be useful later */
	string = xml_text2xmlChar(data);

	utf8string = pg_do_encoding_conversion(string,
										   len,
										   encoding,
										   PG_UTF8);

	/* Start up libxml and its parser (no-ops if already done) */
	pg_xml_init();
	xmlInitParser();

	ctxt = xmlNewParserCtxt();
	if (ctxt == NULL)
		xml_ereport(ERROR, ERRCODE_OUT_OF_MEMORY,
					"could not allocate parser context");

	/* Use a TRY block to ensure the ctxt is released */
	PG_TRY();
	{
		if (xmloption_arg == XMLOPTION_DOCUMENT)
		{
			/*
			 * Note, that here we try to apply DTD defaults
			 * (XML_PARSE_DTDATTR) according to SQL/XML:2008 GR 10.16.7.d:
			 * 'Default values defined by internal DTD are applied'. As for
			 * external DTDs, we try to support them too, (see SQL/XML:2008 GR
			 * 10.16.7.e)
			 */
			doc = xmlCtxtReadDoc(ctxt, utf8string,
								 NULL,
								 "UTF-8",
								 XML_PARSE_NOENT | XML_PARSE_DTDATTR
						   | (preserve_whitespace ? 0 : XML_PARSE_NOBLANKS));
			if (doc == NULL)
				xml_ereport(ERROR, ERRCODE_INVALID_XML_DOCUMENT,
							"invalid XML document");
		}
		else
		{
			int			res_code;
			size_t		count = 0;
			xmlChar    *version;
			int			standalone;

			res_code = parse_xml_decl(utf8string,
									  &count, &version, NULL, &standalone);
			if (res_code != 0)
				xml_ereport_by_code(ERROR, ERRCODE_INVALID_XML_CONTENT,
							  "invalid XML content: invalid XML declaration",
									res_code);

			doc = xmlNewDoc(version);
			Assert(doc->encoding == NULL);
			doc->encoding = xmlStrdup((const xmlChar *) "UTF-8");
			doc->standalone = standalone;

			res_code = xmlParseBalancedChunkMemory(doc, NULL, NULL, 0,
												   utf8string + count, NULL);
			if (res_code != 0)
			{
				xmlFreeDoc(doc);
				xml_ereport(ERROR, ERRCODE_INVALID_XML_CONTENT,
							"invalid XML content");
			}
		}
	}
	PG_CATCH();
	{
		xmlFreeParserCtxt(ctxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	xmlFreeParserCtxt(ctxt);

	return doc;
}


/*
 * xmlChar<->text conversions
 */
static xmlChar *
xml_text2xmlChar(text *in)
{
	return (xmlChar *) text_to_cstring(in);
}


#ifdef USE_LIBXMLCONTEXT

/*
 * Manage the special context used for all libxml allocations (but only
 * in special debug builds; see notes at top of file)
 */
static void
xml_memory_init(void)
{
	/* Create memory context if not there already */
	if (LibxmlContext == NULL)
		LibxmlContext = AllocSetContextCreate(TopMemoryContext,
											  "LibxmlContext",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	/* Re-establish the callbacks even if already set */
	xmlMemSetup(xml_pfree, xml_palloc, xml_repalloc, xml_pstrdup);
}

/*
 * Wrappers for memory management functions
 */
static void *
xml_palloc(size_t size)
{
	return MemoryContextAlloc(LibxmlContext, size);
}


static void *
xml_repalloc(void *ptr, size_t size)
{
	return repalloc(ptr, size);
}


static void
xml_pfree(void *ptr)
{
	/* At least some parts of libxml assume xmlFree(NULL) is allowed */
	if (ptr)
		pfree(ptr);
}


static char *
xml_pstrdup(const char *string)
{
	return MemoryContextStrdup(LibxmlContext, string);
}
#endif   /* USE_LIBXMLCONTEXT */


/*
 * xmlPgEntityLoader --- entity loader callback function
 *
 * Silently prevent any external entity URL from being loaded.  We don't want
 * to throw an error, so instead make the entity appear to expand to an empty
 * string.
 *
 * We would prefer to allow loading entities that exist in the system's
 * global XML catalog; but the available libxml2 APIs make that a complex
 * and fragile task.  For now, just shut down all external access.
 */
static xmlParserInputPtr
xmlPgEntityLoader(const char *URL, const char *ID,
				  xmlParserCtxtPtr ctxt)
{
	return xmlNewStringInputStream(ctxt, (const xmlChar *) "");
}


/*
 * xml_ereport --- report an XML-related error
 *
 * The "msg" is the SQL-level message; some can be adopted from the SQL/XML
 * standard.  This function adds libxml's native error message, if any, as
 * detail.
 *
 * This is exported for modules that want to share the core libxml error
 * handler.  Note that pg_xml_init() *must* have been called previously.
 */
void
xml_ereport(int level, int sqlcode, const char *msg)
{
	char	   *detail;

	/*
	 * It might seem that we should just pass xml_err_buf->data directly to
	 * errdetail.  However, we want to clean out xml_err_buf before throwing
	 * error, in case there is another function using libxml further down the
	 * call stack.
	 */
	if (xml_err_buf->len > 0)
	{
		detail = pstrdup(xml_err_buf->data);
		resetStringInfo(xml_err_buf);
	}
	else
		detail = NULL;

	if (detail)
	{
		size_t		len;

		/* libxml error messages end in '\n'; get rid of it */
		len = strlen(detail);
		if (len > 0 && detail[len - 1] == '\n')
			detail[len - 1] = '\0';

		ereport(level,
				(errcode(sqlcode),
				 errmsg("%s", msg),
				 errdetail("%s", detail)));
	}
	else
	{
		ereport(level,
				(errcode(sqlcode),
				 errmsg("%s", msg)));
	}
}


/*
 * Error handler for libxml error messages
 */
static void
xml_errorHandler(void *ctxt, const char *msg,...)
{
	/* Append the formatted text to xml_err_buf */
	for (;;)
	{
		va_list		args;
		bool		success;

		/* Try to format the data. */
		va_start(args, msg);
		success = appendStringInfoVA(xml_err_buf, msg, args);
		va_end(args);

		if (success)
			break;

		/* Double the buffer size and try again. */
		enlargeStringInfo(xml_err_buf, xml_err_buf->maxlen);
	}
}


/*
 * Wrapper for "ereport" function for XML-related errors.  The "msg"
 * is the SQL-level message; some can be adopted from the SQL/XML
 * standard.  This function uses "code" to create a textual detail
 * message.  At the moment, we only need to cover those codes that we
 * may raise in this file.
 */
static void
xml_ereport_by_code(int level, int sqlcode,
					const char *msg, int code)
{
	const char *det;

	switch (code)
	{
		case XML_ERR_INVALID_CHAR:
			det = gettext_noop("Invalid character value.");
			break;
		case XML_ERR_SPACE_REQUIRED:
			det = gettext_noop("Space required.");
			break;
		case XML_ERR_STANDALONE_VALUE:
			det = gettext_noop("standalone accepts only 'yes' or 'no'.");
			break;
		case XML_ERR_VERSION_MISSING:
			det = gettext_noop("Malformed declaration: missing version.");
			break;
		case XML_ERR_MISSING_ENCODING:
			det = gettext_noop("Missing encoding in text declaration.");
			break;
		case XML_ERR_XMLDECL_NOT_FINISHED:
			det = gettext_noop("Parsing XML declaration: '?>' expected.");
			break;
		default:
			det = gettext_noop("Unrecognized libxml error code: %d.");
			break;
	}

	ereport(level,
			(errcode(sqlcode),
			 errmsg("%s", msg),
			 errdetail(det, code)));
}

#endif   /* USE_LIBXML */


/*
 * XPath related functions
 */

#ifdef USE_LIBXML
/*
 * Convert XML node to text (dump subtree in case of element,
 * return value otherwise)
 */
static text *
xml_xmlnodetoxmltype(xmlNodePtr cur)
{
	xmltype    *result;

	if (cur->type == XML_ELEMENT_NODE)
	{
		xmlBufferPtr buf;

		buf = xmlBufferCreate();
		PG_TRY();
		{
			xmlNodeDump(buf, NULL, cur, 0, 1);
			result = xmlBuffer_to_xmltype(buf);
		}
		PG_CATCH();
		{
			xmlBufferFree(buf);
			PG_RE_THROW();
		}
		PG_END_TRY();
		xmlBufferFree(buf);
	}
	else
	{
		xmlChar    *str;

		str = xmlXPathCastNodeToString(cur);
		PG_TRY();
		{
			result = (xmltype *) cstring_to_text((char *) str);
		}
		PG_CATCH();
		{
			xmlFree(str);
			PG_RE_THROW();
		}
		PG_END_TRY();
		xmlFree(str);
	}

	return result;
}
#endif


/*
 * Common code for xpath() and xmlexists()
 *
 * Evaluate XPath expression and return number of nodes in res_items
 * and array of XML values in astate.
 *
 * It is up to the user to ensure that the XML passed is in fact
 * an XML document - XPath doesn't work easily on fragments without
 * a context node being known.
 */
#ifdef USE_LIBXML
static void
xpath_internal(text *xpath_expr_text, xmltype *data, ArrayType *namespaces,
			   int *res_nitems, ArrayBuildState **astate)
{
	xmlParserCtxtPtr ctxt = NULL;
	xmlDocPtr	doc = NULL;
	xmlXPathContextPtr xpathctx = NULL;
	xmlXPathCompExprPtr xpathcomp = NULL;
	xmlXPathObjectPtr xpathobj = NULL;
	char	   *datastr;
	int32		len;
	int32		xpath_len;
	xmlChar    *string;
	xmlChar    *xpath_expr;
	int			i;
	int			ndim;
	Datum	   *ns_names_uris;
	bool	   *ns_names_uris_nulls;
	int			ns_count;

	/*
	 * Namespace mappings are passed as text[].  If an empty array is passed
	 * (ndim = 0, "0-dimensional"), then there are no namespace mappings.
	 * Else, a 2-dimensional array with length of the second axis being equal
	 * to 2 should be passed, i.e., every subarray contains 2 elements, the
	 * first element defining the name, the second one the URI.  Example:
	 * ARRAY[ARRAY['myns', 'http://example.com'], ARRAY['myns2',
	 * 'http://example2.com']].
	 */
	ndim = namespaces ? ARR_NDIM(namespaces) : 0;
	if (ndim != 0)
	{
		int		   *dims;

		dims = ARR_DIMS(namespaces);

		if (ndim != 2 || dims[1] != 2)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("invalid array for XML namespace mapping"),
					 errdetail("The array must be two-dimensional with length of the second axis equal to 2.")));

		Assert(ARR_ELEMTYPE(namespaces) == TEXTOID);

		deconstruct_array(namespaces, TEXTOID, -1, false, 'i',
						  &ns_names_uris, &ns_names_uris_nulls,
						  &ns_count);

		Assert((ns_count % 2) == 0);	/* checked above */
		ns_count /= 2;			/* count pairs only */
	}
	else
	{
		ns_names_uris = NULL;
		ns_names_uris_nulls = NULL;
		ns_count = 0;
	}

	datastr = VARDATA(data);
	len = VARSIZE(data) - VARHDRSZ;
	xpath_len = VARSIZE(xpath_expr_text) - VARHDRSZ;
	if (xpath_len == 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("empty XPath expression")));

	string = (xmlChar *) palloc((len + 1) * sizeof(xmlChar));
	memcpy(string, datastr, len);
	string[len] = '\0';

	xpath_expr = (xmlChar *) palloc((xpath_len + 1) * sizeof(xmlChar));
	memcpy(xpath_expr, VARDATA(xpath_expr_text), xpath_len);
	xpath_expr[xpath_len] = '\0';

	pg_xml_init();
	xmlInitParser();

	PG_TRY();
	{
		/*
		 * redundant XML parsing (two parsings for the same value during one
		 * command execution are possible)
		 */
		ctxt = xmlNewParserCtxt();
		if (ctxt == NULL)
			xml_ereport(ERROR, ERRCODE_OUT_OF_MEMORY,
						"could not allocate parser context");
		doc = xmlCtxtReadMemory(ctxt, (char *) string, len, NULL, NULL, 0);
		if (doc == NULL)
			xml_ereport(ERROR, ERRCODE_INVALID_XML_DOCUMENT,
						"could not parse XML document");
		xpathctx = xmlXPathNewContext(doc);
		if (xpathctx == NULL)
			xml_ereport(ERROR, ERRCODE_OUT_OF_MEMORY,
						"could not allocate XPath context");
		xpathctx->node = xmlDocGetRootElement(doc);
		if (xpathctx->node == NULL)
			xml_ereport(ERROR, ERRCODE_INTERNAL_ERROR,
						"could not find root XML element");

		/* register namespaces, if any */
		if (ns_count > 0)
		{
			for (i = 0; i < ns_count; i++)
			{
				char	   *ns_name;
				char	   *ns_uri;

				if (ns_names_uris_nulls[i * 2] ||
					ns_names_uris_nulls[i * 2 + 1])
					ereport(ERROR,
							(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					  errmsg("neither namespace name nor URI may be null")));
				ns_name = TextDatumGetCString(ns_names_uris[i * 2]);
				ns_uri = TextDatumGetCString(ns_names_uris[i * 2 + 1]);
				if (xmlXPathRegisterNs(xpathctx,
									   (xmlChar *) ns_name,
									   (xmlChar *) ns_uri) != 0)
					ereport(ERROR,		/* is this an internal error??? */
							(errmsg("could not register XML namespace with name \"%s\" and URI \"%s\"",
									ns_name, ns_uri)));
			}
		}

		xpathcomp = xmlXPathCompile(xpath_expr);
		if (xpathcomp == NULL)	/* TODO: show proper XPath error details */
			xml_ereport(ERROR, ERRCODE_INTERNAL_ERROR,
						"invalid XPath expression");

		/*
		 * Version 2.6.27 introduces a function named
		 * xmlXPathCompiledEvalToBoolean, which would be enough for xmlexists,
		 * but we can derive the existence by whether any nodes are returned,
		 * thereby preventing a library version upgrade and keeping the code
		 * the same.
		 */
		xpathobj = xmlXPathCompiledEval(xpathcomp, xpathctx);
		if (xpathobj == NULL)	/* TODO: reason? */
			xml_ereport(ERROR, ERRCODE_INTERNAL_ERROR,
						"could not create XPath object");

		/* return empty array in cases when nothing is found */
		if (xpathobj->nodesetval == NULL)
			*res_nitems = 0;
		else
			*res_nitems = xpathobj->nodesetval->nodeNr;

		if (*res_nitems && astate)
		{
			*astate = NULL;
			for (i = 0; i < xpathobj->nodesetval->nodeNr; i++)
			{
				Datum		elem;
				bool		elemisnull = false;

				elem = PointerGetDatum(xml_xmlnodetoxmltype(xpathobj->nodesetval->nodeTab[i]));
				*astate = accumArrayResult(*astate, elem,
										   elemisnull, XMLOID,
										   CurrentMemoryContext);
			}
		}
	}
	PG_CATCH();
	{
		if (xpathobj)
			xmlXPathFreeObject(xpathobj);
		if (xpathcomp)
			xmlXPathFreeCompExpr(xpathcomp);
		if (xpathctx)
			xmlXPathFreeContext(xpathctx);
		if (doc)
			xmlFreeDoc(doc);
		if (ctxt)
			xmlFreeParserCtxt(ctxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	xmlXPathFreeObject(xpathobj);
	xmlXPathFreeCompExpr(xpathcomp);
	xmlXPathFreeContext(xpathctx);
	xmlFreeDoc(doc);
	xmlFreeParserCtxt(ctxt);
}
#endif   /* USE_LIBXML */

/*
 * Evaluate XPath expression and return array of XML values.
 *
 * As we have no support of XQuery sequences yet, this function seems
 * to be the most useful one (array of XML functions plays a role of
 * some kind of substitution for XQuery sequences).
 */
Datum
xpath(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	text	   *xpath_expr_text = PG_GETARG_TEXT_P(0);
	xmltype    *data = PG_GETARG_XML_P(1);
	ArrayType  *namespaces = PG_GETARG_ARRAYTYPE_P(2);
	int			res_nitems;
	ArrayBuildState *astate;

	xpath_internal(xpath_expr_text, data, namespaces,
				   &res_nitems, &astate);

	if (res_nitems == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(XMLOID));
	else
		PG_RETURN_DATUM(makeArrayResult(astate, CurrentMemoryContext));
#else
	NO_XML_SUPPORT();
	return 0;
#endif
}

/*
 * Determines if the node specified by the supplied XPath exists
 * in a given XML document, returning a boolean.
 */
Datum
xmlexists(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	text	   *xpath_expr_text = PG_GETARG_TEXT_P(0);
	xmltype    *data = PG_GETARG_XML_P(1);
	int			res_nitems;

	xpath_internal(xpath_expr_text, data, NULL,
				   &res_nitems, NULL);

	PG_RETURN_BOOL(res_nitems > 0);
#else
	NO_XML_SUPPORT();
	return 0;
#endif
}

/*
 * Determines if the node specified by the supplied XPath exists
 * in a given XML document, returning a boolean. Differs from
 * xmlexists as it supports namespaces and is not defined in SQL/XML.
 */
Datum
xpath_exists(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	text	   *xpath_expr_text = PG_GETARG_TEXT_P(0);
	xmltype    *data = PG_GETARG_XML_P(1);
	ArrayType  *namespaces = PG_GETARG_ARRAYTYPE_P(2);
	int			res_nitems;

	xpath_internal(xpath_expr_text, data, namespaces,
				   &res_nitems, NULL);

	PG_RETURN_BOOL(res_nitems > 0);
#else
	NO_XML_SUPPORT();
	return 0;
#endif
}

/*
 * Functions for checking well-formed-ness
 */

#ifdef USE_LIBXML
static bool
wellformed_xml(text *data, XmlOptionType xmloption_arg)
{
	bool		result;
	xmlDocPtr	doc = NULL;

	/* We want to catch any exceptions and return false */
	PG_TRY();
	{
		doc = xml_parse(data, xmloption_arg, true, GetDatabaseEncoding());
		result = true;
	}
	PG_CATCH();
	{
		FlushErrorState();
		result = false;
	}
	PG_END_TRY();

	if (doc)
		xmlFreeDoc(doc);

	return result;
}
#endif

Datum
xml_is_well_formed(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	text	   *data = PG_GETARG_TEXT_P(0);

	PG_RETURN_BOOL(wellformed_xml(data, xmloption));
#else
	NO_XML_SUPPORT();
	return 0;
#endif   /* not USE_LIBXML */
}

Datum
xml_is_well_formed_document(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	text	   *data = PG_GETARG_TEXT_P(0);

	PG_RETURN_BOOL(wellformed_xml(data, XMLOPTION_DOCUMENT));
#else
	NO_XML_SUPPORT();
	return 0;
#endif   /* not USE_LIBXML */
}

Datum
xml_is_well_formed_content(PG_FUNCTION_ARGS)
{
#ifdef USE_LIBXML
	text	   *data = PG_GETARG_TEXT_P(0);

	PG_RETURN_BOOL(wellformed_xml(data, XMLOPTION_CONTENT));
#else
	NO_XML_SUPPORT();
	return 0;
#endif   /* not USE_LIBXML */
}
