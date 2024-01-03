%{
#include "postgres.h"


#include "nodes/pg_list.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "parser/scanner.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/datetime.h"

#include "access/paxc_scanner.h"

/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
        do { \
                if (N) \
                        (Current) = (Rhs)[1]; \
                else \
                        (Current) = (Rhs)[0]; \
        } while (0)

#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)
#define parser_yyerror(msg)  scanner_yyerror(yyscanner, msg)

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

static void paxc_yyerror(core_yyscan_t yyscanner, const char *message);
static int paxc_yylex(core_yyscan_t yyscanner);
static int paxc_scanner_errposition(int location);
static List *paxc_result;

%}

/* %pure-parser */
%expect 0
%name-prefix="paxc_yy"
%locations
%parse-param {core_yyscan_t yyscanner}
%lex-param   {core_yyscan_t yyscanner}

%union
{
    core_YYSTYPE            core_yystype;
    /* these fields must match core_YYSTYPE: */
    int                              ival;
    char                            *str;
    const char                      *keyword;

    bool                            boolean;
    List                            *list;
    Node                            *node;
    TypeName                        *typnam;
    PartitionElem           *partelem;
    PartitionSpec           *partspec;
    PartitionBoundSpec      *partboundspec;
}

/* %type <list> top_level_stmt */
%type <list> partition_by part_params any_name opt_collate attrs opt_qualified_name
%type <partelem>        part_elem
%type <str> ColId attr_name

// FIXME: types for partition ranges
//%type <list> partition_ranges expr_list opt_type_modifiers
//%type <partboundspec> partition_range
//%type <node> AexprConst a_expr c_expr
//%type <typnam> Numeric opt_float ConstTypename ConstDatetime ConstInterval ConstCharacter CharacterWithLength CharacterWithoutLength ConstBit BitWithLength BitWithoutLength
//%type <boolean> opt_varying opt_timezone
//%type <list>    opt_interval interval_second
//%type <str>  Sconst character
//%type <ival> Iconst

%token <str>    IDENT
//%token <str>    FCONST SCONST BCONST XCONST
//%token <ival>   ICONST 

%token <keyword> COLLATE
//%token <keyword> TRUE_P FALSE_P HOUR_P YEAR_P NULL_P MONTH_P TO VARYING VARCHAR TIMESTAMP BIT TIME INTERVAL DAY_P MINUTE_P SECOND_P CHARACTER NATIONAL NCHAR CHAR_P ZONE INT_P INTEGER SMALLINT BIGINT REAL FLOAT_P DOUBLE_P PRECISION DECIMAL_P DEC NUMERIC BOOLEAN_P FROM

%token WITH_LA WITHOUT_LA


%%

top_level_stmt:
    partition_by { paxc_result = $1; }
//    | partition_ranges { paxc_result = $1; }
    ;

partition_by: part_params { $$ = $1; }
    ;
part_params:
      part_elem                                     { $$ = list_make1($1); }
    | part_params ',' part_elem                     { $$ = lappend($1, $3); }
    ;

part_elem: ColId opt_collate opt_qualified_name
            {
                PartitionElem *n = makeNode(PartitionElem);

                n->name = $1;
                n->expr = NULL;
                n->collation = $2;
                n->opclass = $3;
                n->location = @1;
                $$ = n;
            }
/*
                        | func_expr_windowless opt_collate opt_qualified_name
                                {
                                        PartitionElem *n = makeNode(PartitionElem);

                                        n->name = NULL;
                                        n->expr = $1;
                                        n->collation = $2;
                                        n->opclass = $3;
                                        n->location = @1;
                                        $$ = n;
                                }
                        | '(' a_expr ')' opt_collate opt_qualified_name
                                {
                                        PartitionElem *n = makeNode(PartitionElem);

                                        n->name = NULL;
                                        n->expr = $2;
                                        n->collation = $4;
                                        n->opclass = $5;
                                        n->location = @1;
                                        $$ = n;
                                }
*/
    ;

/* Column identifier --- names that can be column, table, etc names.
 */
ColId: IDENT  { $$ = $1; }
    ;
opt_collate: COLLATE any_name   { $$ = $2; }
    | /*EMPTY*/             { $$ = NIL; }
    ;

any_name:
      ColId                 { $$ = list_make1(makeString($1)); }
    | ColId attrs           { $$ = lcons(makeString($1), $2); }
    ;

attrs: '.' attr_name        { $$ = list_make1(makeString($2)); }
    | attrs '.' attr_name   { $$ = lappend($1, makeString($3)); }
    ;

attr_name: IDENT { $$ = $1; }
    ;

/* opclass */
opt_qualified_name: any_name    { $$ = $1; }
    | /*EMPTY*/                 { $$ = NIL; }
    ;

//partition_ranges: partition_ranges ',' partition_range { $$ = lappend($1, $3); }
//    | partition_range { $$ = list_make1($1); }
//    ;
//
//partition_range: FROM '(' expr_list ')' TO '(' expr_list ')'
//    {
//        PartitionBoundSpec *n = makeNode(PartitionBoundSpec);
//
//        n->strategy = PARTITION_STRATEGY_RANGE;
//        n->is_default = false;
//        n->lowerdatums = $3;
//        n->upperdatums = $7;
//
//        $$ = n;
//    }
//    ;
//
//expr_list:  a_expr { $$ = list_make1($1); }
//    | expr_list ',' a_expr { $$ = lappend($1, $3); }
//    ;
//
//a_expr: c_expr      { $$ = $1; }
//    ;
//c_expr: AexprConst  { $$ = $1; }
//    ;
//
///*
// * Constants
// */
//AexprConst: Iconst { $$ = makeIntConst($1, @1); }
//    | FCONST { $$ = makeFloatConst($1, @1); }
//    | Sconst { $$ = makeStringConst($1, @1); }
//    | BCONST { $$ = makeBitStringConst($1, @1); }
//    | XCONST
//        {
//            /* This is a bit constant per SQL99:
//             * Without Feature F511, "BIT data type",
//             * a <general literal> shall not be a
//             * <bit string literal> or a <hex string literal>.
//             */
//            $$ = makeBitStringConst($1, @1);
//        }
//    | ConstTypename Sconst { $$ = makeStringConstCast($2, @2, $1); }
//    | ConstInterval Sconst opt_interval
//        {
//            TypeName   *t = $1;
//
//            t->typmods = $3;
//            $$ = makeStringConstCast($2, @2, t);
//        }
//    | ConstInterval '(' Iconst ')' Sconst
//        {
//            TypeName   *t = $1;
//
//            t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
//                                    makeIntConst($3, @3));
//            $$ = makeStringConstCast($5, @5, t);
//        }
//    | TRUE_P { $$ = makeBoolAConst(true, @1); }
//    | FALSE_P { $$ = makeBoolAConst(false, @1); }
//    | NULL_P { $$ = makeNullAConst(@1); }
//    ;
//
//Iconst: ICONST      { $$ = $1; };
//Sconst: SCONST      { $$ = $1; };
//
//ConstTypename:
//      Numeric           { $$ = $1; }
//    | ConstBit          { $$ = $1; }
//    | ConstCharacter    { $$ = $1; }
//    | ConstDatetime     { $$ = $1; }
//    ;
//
///* ConstBit is like Bit except "BIT" defaults to unspecified length */
///* See notes for ConstCharacter, which addresses same issue for "CHAR" */
//ConstBit: BitWithLength { $$ = $1; }
//    | BitWithoutLength
//        {
//            $$ = $1;
//            $$->typmods = NIL;
//        }
//    ;
//
//BitWithLength: BIT opt_varying '(' expr_list ')'
//    {
//        char *typname;
//
//        typname = $2 ? "varbit" : "bit";
//        $$ = SystemTypeName(typname);
//        $$->typmods = $4;
//        $$->location = @1;
//    }
//    ;
//
//BitWithoutLength: BIT opt_varying
//    {
//        /* bit defaults to bit(1), varbit to no limit */
//        if ($2)
//        {
//            $$ = SystemTypeName("varbit");
//        }
//        else
//        {
//            $$ = SystemTypeName("bit");
//            $$->typmods = list_make1(makeIntConst(1, -1));
//        }
//        $$->location = @1;
//    }
//    ;
//
//ConstCharacter:  CharacterWithLength
//        {
//            $$ = $1;
//        }
//    | CharacterWithoutLength
//        {
//            /* Length was not specified so allow to be unrestricted.
//             * This handles problems with fixed-length (bpchar) strings
//             * which in column definitions must default to a length
//             * of one, but should not be constrained if the length
//             * was not specified.
//             */
//            $$ = $1;
//            $$->typmods = NIL;
//        }
//    ;
//
//CharacterWithLength:  character '(' Iconst ')'
//    {
//        $$ = SystemTypeName($1);
//        $$->typmods = list_make1(makeIntConst($3, @3));
//        $$->location = @1;
//    }
//    ;
//
//CharacterWithoutLength:  character
//    {
//        $$ = SystemTypeName($1);
//        /* char defaults to char(1), varchar to no limit */
//        if (strcmp($1, "bpchar") == 0)
//            $$->typmods = list_make1(makeIntConst(1, -1));
//        $$->location = @1;
//    }
//    ;
//
//character: CHARACTER opt_varying { $$ = $2 ? "varchar": "bpchar"; }
//    | CHAR_P opt_varying { $$ = $2 ? "varchar": "bpchar"; }
//    | VARCHAR { $$ = "varchar"; }
//    | NATIONAL CHARACTER opt_varying { $$ = $3 ? "varchar": "bpchar"; }
//    | NATIONAL CHAR_P opt_varying { $$ = $3 ? "varchar": "bpchar"; }
//    | NCHAR opt_varying { $$ = $2 ? "varchar": "bpchar"; }
//    ;
//
//opt_varying: VARYING    { $$ = true; }
//    | /*EMPTY*/         { $$ = false; }
//    ;
//
///*
// * SQL date/time types
// */
//ConstDatetime:
//    TIMESTAMP '(' Iconst ')' opt_timezone
//        {
//            if ($5)
//                $$ = SystemTypeName("timestamptz");
//            else
//                $$ = SystemTypeName("timestamp");
//            $$->typmods = list_make1(makeIntConst($3, @3));
//            $$->location = @1;
//        }
//    | TIMESTAMP opt_timezone
//        {
//            if ($2)
//                $$ = SystemTypeName("timestamptz");
//            else
//                $$ = SystemTypeName("timestamp");
//            $$->location = @1;
//        }
//    | TIME '(' Iconst ')' opt_timezone
//        {
//            if ($5)
//                $$ = SystemTypeName("timetz");
//            else
//                $$ = SystemTypeName("time");
//            $$->typmods = list_make1(makeIntConst($3, @3));
//            $$->location = @1;
//        }
//    | TIME opt_timezone
//        {
//            if ($2)
//                $$ = SystemTypeName("timetz");
//            else
//                $$ = SystemTypeName("time");
//            $$->location = @1;
//        }
//    ;
//
//ConstInterval: INTERVAL
//    {
//        $$ = SystemTypeName("interval");
//        $$->location = @1;
//    }
//    ;
//
//opt_timezone: WITH_LA TIME ZONE     { $$ = true; }
//    | WITHOUT_LA TIME ZONE          { $$ = false; }
//    | /*EMPTY*/                     { $$ = false; }
//    ;
//
//opt_interval:
//      YEAR_P { $$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR), @1)); }
//    | MONTH_P { $$ = list_make1(makeIntConst(INTERVAL_MASK(MONTH), @1)); }
//    | DAY_P { $$ = list_make1(makeIntConst(INTERVAL_MASK(DAY), @1)); }
//    | HOUR_P { $$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR), @1)); }
//    | MINUTE_P { $$ = list_make1(makeIntConst(INTERVAL_MASK(MINUTE), @1)); }
//    | interval_second { $$ = $1; }
//    | YEAR_P TO MONTH_P
//        {
//            $$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR) |
//                                         INTERVAL_MASK(MONTH), @1));
//        }
//    | DAY_P TO HOUR_P
//        {
//            $$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
//                                         INTERVAL_MASK(HOUR), @1));
//        }
//    | DAY_P TO MINUTE_P
//        {
//            $$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
//                                         INTERVAL_MASK(HOUR) |
//                                         INTERVAL_MASK(MINUTE), @1));
//        }
//    | DAY_P TO interval_second
//        {
//            $$ = $3;
//            linitial($$) = makeIntConst(INTERVAL_MASK(DAY) |
//                                        INTERVAL_MASK(HOUR) |
//                                        INTERVAL_MASK(MINUTE) |
//                                        INTERVAL_MASK(SECOND), @1);
//        }
//    | HOUR_P TO MINUTE_P
//        {
//            $$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR) |
//                                         INTERVAL_MASK(MINUTE), @1));
//        }
//    | HOUR_P TO interval_second
//        {
//            $$ = $3;
//            linitial($$) = makeIntConst(INTERVAL_MASK(HOUR) |
//                                        INTERVAL_MASK(MINUTE) |
//                                        INTERVAL_MASK(SECOND), @1);
//        }
//    | MINUTE_P TO interval_second
//        {
//            $$ = $3;
//            linitial($$) = makeIntConst(INTERVAL_MASK(MINUTE) |
//                                        INTERVAL_MASK(SECOND), @1);
//        }
//    | /*EMPTY*/ { $$ = NIL; }
//    ;
//
//interval_second:
//    SECOND_P
//        {
//            $$ = list_make1(makeIntConst(INTERVAL_MASK(SECOND), @1));
//        }
//    | SECOND_P '(' Iconst ')'
//        {
//            $$ = list_make2(makeIntConst(INTERVAL_MASK(SECOND), @1),
//                            makeIntConst($3, @3));
//        }
//    ;
//
//opt_type_modifiers: '(' expr_list ')'       { $$ = $2; }
//    | /* EMPTY */                           { $$ = NIL; }
//    ;
//
///*
// * SQL numeric data types
// */
//Numeric:
//      INT_P
//        {
//                $$ = SystemTypeName("int4");
//                $$->location = @1;
//        }
//    | INTEGER
//        {
//                $$ = SystemTypeName("int4");
//                $$->location = @1;
//        }
//    | SMALLINT
//        {
//                $$ = SystemTypeName("int2");
//                $$->location = @1;
//        }
//    | BIGINT
//        {
//                $$ = SystemTypeName("int8");
//                $$->location = @1;
//        }
//    | REAL
//        {
//                $$ = SystemTypeName("float4");
//                $$->location = @1;
//        }
//    | FLOAT_P opt_float
//        {
//                $$ = $2;
//                $$->location = @1;
//        }
//    | DOUBLE_P PRECISION
//        {
//                $$ = SystemTypeName("float8");
//                $$->location = @1;
//        }
//    | DECIMAL_P opt_type_modifiers
//        {
//                $$ = SystemTypeName("numeric");
//                $$->typmods = $2;
//                $$->location = @1;
//        }
//    | DEC opt_type_modifiers
//        {
//                $$ = SystemTypeName("numeric");
//                $$->typmods = $2;
//                $$->location = @1;
//        }
//    | NUMERIC opt_type_modifiers
//        {
//                $$ = SystemTypeName("numeric");
//                $$->typmods = $2;
//                $$->location = @1;
//        }
//    | BOOLEAN_P
//        {
//                $$ = SystemTypeName("bool");
//                $$->location = @1;
//        }
//    ;
//
//opt_float:      '(' Iconst ')'
//        {
//                /*
//                 * Check FLOAT() precision limits assuming IEEE floating
//                 * types - thomas 1997-09-18
//                 */
//                if ($2 < 1)
//                        ereport(ERROR,
//                                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
//                                         errmsg("precision for type float must be at least 1 bit"),
//                                         parser_errposition(@2)));
//                else if ($2 <= 24)
//                        $$ = SystemTypeName("float4");
//                else if ($2 <= 53)
//                        $$ = SystemTypeName("float8");
//                else
//                        ereport(ERROR,
//                                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
//                                         errmsg("precision for type float must be less than 54 bits"),
//                                         parser_errposition(@2)));
//        }
//    | /*EMPTY*/ { $$ = SystemTypeName("float8"); }
//    ;
//

%%

static int paxc_scanner_errposition(int location) {
  return location;
}

static void paxc_yyerror(core_yyscan_t yyscanner, const char *message) {
  ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                  errmsg("%s", _(message))));
}
static int paxc_yylex(core_yyscan_t yyscanner) {
  return core_yylex(&paxc_yylval.core_yystype, &paxc_yylloc, yyscanner);
}

static core_yyscan_t paxc_scanner_init(const char *str, core_yy_extra_type *extra) {
  paxc_result = NIL;
  return scanner_init(str, extra, &ScanKeywords, ScanKeywordTokens);
}

static void paxc_scanner_finish(core_yyscan_t yyscanner) {
  scanner_finish(yyscanner);
  paxc_result = NIL;
}

List *paxc_raw_parse(const char *str) {
  core_yyscan_t yyscanner;
  core_yy_extra_type extra;
  List *result;
  int                     yyresult;

  yyscanner = paxc_scanner_init(str, &extra);
  yyresult = paxc_yyparse(yyscanner);
  if (yyresult != 0)
    elog(ERROR, "pacx_yyparse returned %d", yyresult);

  result = paxc_result;
  paxc_scanner_finish(yyscanner);
  return result;
}

