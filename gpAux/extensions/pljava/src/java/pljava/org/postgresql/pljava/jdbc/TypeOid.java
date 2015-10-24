/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.jdbc;

import org.postgresql.pljava.internal.Oid;

/**
 * Provides constants for well-known backend OIDs for the types we commonly
 * use.
 */
public class TypeOid {
    public static final Oid INVALID = new Oid(0);
    public static final Oid INT2 = new Oid(21);
    public static final Oid INT4 = new Oid(23);
    public static final Oid INT8 = new Oid(20);
    public static final Oid TEXT = new Oid(25);
    public static final Oid NUMERIC = new Oid(1700);
    public static final Oid FLOAT4 = new Oid(700);
    public static final Oid FLOAT8 = new Oid(701);
    public static final Oid BOOL = new Oid(16);
    public static final Oid DATE = new Oid(1082);
    public static final Oid TIME = new Oid(1083);
    public static final Oid TIMESTAMP = new Oid(1114);
    public static final Oid TIMESTAMPTZ = new Oid(1184);
    public static final Oid BYTEA = new Oid(17);
    public static final Oid VARCHAR = new Oid(1043);
    public static final Oid OID = new Oid(26);
    public static final Oid BPCHAR = new Oid(1042);
}
