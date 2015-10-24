/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

/**
 * PostgreSQL builtin functions
 * @author Filip Hrbek
 */
public class BuiltinFunctions {
    // numeric functions names
    public final static String ABS="abs";
    public final static String ACOS="acos";
    public final static String ASIN="asin";
    public final static String ATAN="atan";
    public final static String ATAN2="atan2";
    public final static String CEILING="ceiling";
    public final static String COS="cos";
    public final static String COT="cot";
    public final static String DEGREES="degrees";
    public final static String EXP="exp";
    public final static String FLOOR="floor";
    public final static String LOG="log";
    public final static String LOG10="log10";
    public final static String MOD="mod";
    public final static String PI="pi";
    public final static String POWER="power";
    public final static String RADIANS="radians";
    public final static String RAND="rand";
    public final static String ROUND="round";
    public final static String SIGN="sign";
    public final static String SIN="sin";
    public final static String SQRT="sqrt";
    public final static String TAN="tan";
    public final static String TRUNCATE="truncate";
    
    // string function names   
    public final static String ASCII="ascii";
    public final static String CHAR="char";
    public final static String CONCAT="concat";
    public final static String INSERT="insert"; // change arguments order
    public final static String LCASE="lcase";
    public final static String LEFT="left";
    public final static String LENGTH="length";
    public final static String LOCATE="locate"; // the 3 args version duplicate args
    public final static String LTRIM="ltrim";
    public final static String REPEAT="repeat";
    public final static String REPLACE="replace";
    public final static String RIGHT="right"; // duplicate args
    public final static String RTRIM="rtrim";
    public final static String SPACE="space";
    public final static String SUBSTRING="substring";
    public final static String UCASE="ucase";
    // soundex is implemented on the server side by 
    // the contrib/fuzzystrmatch module.  We provide a translation
    // for this in the driver, but since we don't want to bother with run
    // time detection of this module's installation we don't report this
    // method as supported in DatabaseMetaData.
    // difference is currently unsupported entirely.

    // date time function names
    public final static String CURDATE="curdate";
    public final static String CURTIME="curtime";
    public final static String DAYNAME="dayname";
    public final static String DAYOFMONTH="dayofmonth";
    public final static String DAYOFWEEK="dayofweek";
    public final static String DAYOFYEAR="dayofyear";
    public final static String HOUR="hour";
    public final static String MINUTE="minute";
    public final static String MONTH="month";
    public final static String MONTHNAME="monthname";
    public final static String NOW="now";
    public final static String QUARTER="quarter";
    public final static String SECOND="second";
    public final static String WEEK="week";
    public final static String YEAR="year";
    // TODO : timestampadd and timestampdiff

    // system functions
    public final static String DATABASE="database";
    public final static String IFNULL="ifnull";
    public final static String USER="user";
}
