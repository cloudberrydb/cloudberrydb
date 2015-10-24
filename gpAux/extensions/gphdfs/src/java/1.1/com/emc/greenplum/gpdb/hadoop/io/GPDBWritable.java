package com.emc.greenplum.gpdb.hadoop.io;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.DataInputStream;

import org.apache.hadoop.io.Writable;


/**
 * This class represents a GPDB record in the form of
 * a Java object.
 * @author achoi
 *
 */
public class GPDBWritable implements Writable {
	/*
	 * GPDBWritable is using the following serialization form:
	 * Total Length | Version | #columns | Col type | Col type |... | Null Bit array   | Col val...
     * 4 byte		| 2 byte	2 byte	   1 byte     1 byte	      ceil(#col/8) byte	 fixed length or var len
     * 
     * For fixed length type, we know the length.
     * In the col val, we align pad according to the alignemnt requirement of the type.
     * For var length type, the alignment is always 4 byte.
     * For var legnth type, col val is <4 byte length><payload val>
	 */


	/*
	 * "DEFINE" of the Database Datatype OID
	 * All datatypes are supported. The types listed here
	 * are commonly used type to facilitaes ease-of-use.
	 * Note that String should always be UTF-8 formatted.
	 */
	public static final int BOOLEAN   =   16;
	public static final int BYTEA     =   17;
	public static final int CHAR      =   18;
	public static final int BIGINT    =   20;
	public static final int SMALLINT  =   21;
	public static final int INTEGER   =   23;
	public static final int TEXT      =   25;
	public static final int REAL      =  700;
	public static final int FLOAT8    =  701;
	public static final int BPCHAR    = 1042;
	public static final int VARCHAR   = 1043;
	public static final int DATE      = 1082;
	public static final int TIME      = 1083;
	public static final int TIMESTAMP = 1114;
	public static final int NUMERIC   = 1700;
	
	/*
	 * Enum of the Database type
	 */
	private enum DBType {
		BIGINT(8, 8, false), BOOLEAN(1, 1, false), FLOAT8(8, 8, false), INTEGER(4, 4, false), REAL(4, 4, false),
		SMALLINT(2, 2, false), BYTEA(4, -1, false), TEXT(4, -1, true);
		
		private final int typelength; // -1 means var length
		private final int alignment;
		private final boolean isTextFormat;

		DBType(int align, int len, boolean isbin) {
			this.typelength = len;
			this.alignment  = align;
			this.isTextFormat = isbin;
		}
		
		public int getTypeLength() { return typelength; }
		public boolean isVarLength() { return typelength == -1; }

		// return the alignment requirement of the type
		public int getAlignment() { return alignment; }
	}

	/*
	 * Constants
	 */
	private static final int    VERSION = 1;
	private static final String CHARSET = "UTF-8";
	
	/*
	 * Local variables
	 */
	protected int[]    colType;
	protected Object[] colValue;
	
	/**
	 * An exception class for column type definition and
	 * set/get value mismatch.
	 * @author achoi
	 *
	 */
	public class TypeMismatchException extends IOException {
		public TypeMismatchException(String msg) {
			super(msg);
		}
	}

	/**
	 * Empty Constructor
	 */
	public GPDBWritable() {
	}
	
	/**
	 * Constructor to build a db record. colType defines the schema
	 * @param columnType the table column types
	 */
	public GPDBWritable(int[] columnType) {
		colType = columnType;
		colValue = new Object[columnType.length];
	}
	
	/**
	 * Constructor to build a db record from a serialized form.
	 * @param data a record in the serialized form
	 * @throws IOException if the data is malformatted.
	 */
	public GPDBWritable(byte[] data) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(data);
		DataInputStream      dis = new DataInputStream(bis);

		this.readFields(dis);
	}
	

	/**
	 *  Implement {@link org.apache.hadoop.io.Writable#readFields(DataInput)} 
	 */
	public void readFields(DataInput in) throws IOException {
		/* extract the version and col cnt */
		int pklen   = in.readInt();
		int version = in.readShort();
		int colCnt  = in.readShort();
		int curOffset = 4+2+2;
		
		/* !!! Check VERSION !!! */
		if (version != this.VERSION) {
			throw new IOException("Current GPDBWritable version(" +
					this.VERSION + ") does not match input version(" +
					version+")");
		}
		
		/* Extract Column Type */
		colType = new int[colCnt];
		DBType[] coldbtype = new DBType[colCnt];
		for(int i=0; i<colCnt; i++) {
			int enumType = (int)(in.readByte());
			curOffset += 1;
			if      (enumType == DBType.BIGINT.ordinal())   { colType[i] = BIGINT; coldbtype[i] = DBType.BIGINT;} 
			else if (enumType == DBType.BOOLEAN.ordinal())  { colType[i] = BOOLEAN; coldbtype[i] = DBType.BOOLEAN;}
			else if (enumType == DBType.FLOAT8.ordinal())   { colType[i] = FLOAT8;  coldbtype[i] = DBType.FLOAT8;}
			else if (enumType == DBType.INTEGER.ordinal())  { colType[i] = INTEGER; coldbtype[i] = DBType.INTEGER;}
			else if (enumType == DBType.REAL.ordinal())     { colType[i] = REAL; coldbtype[i] = DBType.REAL;}
			else if (enumType == DBType.SMALLINT.ordinal()) { colType[i] = SMALLINT; coldbtype[i] = DBType.SMALLINT;}
			else if (enumType == DBType.BYTEA.ordinal())    { colType[i] = BYTEA; coldbtype[i] = DBType.BYTEA;}
			else if (enumType == DBType.TEXT.ordinal())     { colType[i] = TEXT; coldbtype[i] = DBType.TEXT; }
			else throw new IOException("Unknown GPDBWritable.DBType ordinal value");
		}
		
		/* Extract null bit array */
		byte[] nullbytes = new byte[getNullByteArraySize(colCnt)];
		in.readFully(nullbytes);
		curOffset += nullbytes.length;
		boolean[] colIsNull = byteArrayToBooleanArray(nullbytes, colCnt);

		/* extract column value */
		colValue = new Object[colCnt];
		for(int i=0; i<colCnt; i++) {
			if (!colIsNull[i]) {
				/* Skip the aligment padding */
				int skipbytes = roundUpAlignment(curOffset, coldbtype[i].getAlignment()) - curOffset;
				for(int j=0; j<skipbytes; j++)
					in.readByte();
				curOffset += skipbytes;

				/* For fixed length type, increment the offset according to type type length here.
				 * For var length type (BYTEA, TEXT), we'll read 4 byte length header and the
				 * actual payload.
				 */
				int varcollen = -1;
				if (coldbtype[i].isVarLength()) {
					varcollen = in.readInt();
					curOffset += 4 + varcollen;
				}
				else
					curOffset += coldbtype[i].getTypeLength();
				
				switch(colType[i]) {
					case BIGINT:   { long    val = in.readLong();    colValue[i] = new Long(val);    break; } 
					case BOOLEAN:  { boolean val = in.readBoolean(); colValue[i] = new Boolean(val); break; }
					case FLOAT8:   { double  val = in.readDouble();  colValue[i] = new Double(val);  break; }
					case INTEGER:  { int     val = in.readInt();     colValue[i] = new Integer(val); break; }
					case REAL:     { float   val = in.readFloat();   colValue[i] = new Float(val);   break; }
					case SMALLINT: { short   val = in.readShort();   colValue[i] = new Short(val);   break; }
					
					/* For BYTEA column, it has a 4 byte var length header. */
					case BYTEA:    { 
						colValue[i] = new byte[varcollen]; 
						in.readFully((byte[])colValue[i]);
						break;
					}
					/* For text formatted column, it has a 4 byte var length header
					 * and it's always null terminated string.
					 * So, we can remove the last "\0" when constructing the string.
					 */
					case TEXT:       {
						byte[] data = new byte[varcollen];
						in.readFully(data, 0, varcollen);
						colValue[i] = new String(data, 0, varcollen-1, CHARSET);
						break;
					}
					
					default:
						throw new IOException("Unknown GPDBWritable ColType");
				}				
			}
		}
		
		/* Skip the ending aligment padding */
		int skipbytes = roundUpAlignment(curOffset, 8) - curOffset;
		for(int j=0; j<skipbytes; j++)
			in.readByte();
		curOffset += skipbytes;
	}

	/**
	 *  Implement {@link org.apache.hadoop.io.Writable#write(DataOutput)} 
	 */
	public void write(DataOutput out) throws IOException {
		int       numCol    = colType.length;
		boolean[] nullBits  = new boolean[numCol];
		int[]     colLength = new int[numCol];
		byte[]    enumType  = new byte[numCol];
		int[]     padLength = new int[numCol];
		byte[]    padbytes   = new byte[8];
		
		/**
		 * Compute the total payload and header length
		 * header = total length (4 byte), Version (2 byte), #col (2 byte)
		 * col type array = #col * 1 byte
		 * null bit array = ceil(#col/8)
		 */
		int datlen = 4+2+2;
		datlen += numCol;
		datlen += getNullByteArraySize(numCol);
		
		for(int i=0; i<numCol; i++) {
			/* Get the enum type */
			DBType coldbtype;
			switch(colType[i]) {
				case BIGINT:   coldbtype = DBType.BIGINT; break;
				case BOOLEAN:  coldbtype = DBType.BOOLEAN; break;
				case FLOAT8:   coldbtype = DBType.FLOAT8; break;
				case INTEGER:  coldbtype = DBType.INTEGER; break;
				case REAL:     coldbtype = DBType.REAL; break;
				case SMALLINT: coldbtype = DBType.SMALLINT; break;
				case BYTEA:    coldbtype = DBType.BYTEA; break;
				default:       coldbtype = DBType.TEXT; 
			}
			enumType[i] = (byte)(coldbtype.ordinal());

			/* Get the actual value, and set the null bit */
			if (colValue[i] == null) {
				nullBits[i]  = true;
				colLength[i] = 0;
			} else {
				nullBits[i]  = false;
								
				/* For binary format and fixed length type, we know it's fixed.
				 * 
				 * For binary format and var length type, the length is in the col value.
				 * 
				 * For text format, we need to add "\0" in the length.
				 */
				if (!isTextForm(colType[i]) && !coldbtype.isVarLength())
					colLength[i] = coldbtype.getTypeLength();
				else if (!isTextForm(colType[i]) && coldbtype.isVarLength())
					colLength[i] = ((byte[])colValue[i]).length;
				else
					colLength[i] = ((String)colValue[i]).getBytes(CHARSET).length + 1;

				/* Add type alignment padding.
				 * For variable length type, we added a 4 byte length header.
				 */
				padLength[i] = roundUpAlignment(datlen, coldbtype.getAlignment()) - datlen;
				datlen += padLength[i];
				if (coldbtype.isVarLength())
					datlen += 4;
			}
			datlen += colLength[i];
		}

		/*
		 * Add the final alignment padding for the next record
		 */
		int endpadding = roundUpAlignment(datlen, 8) - datlen;
		datlen += endpadding;
		
		/* Construct the packet header */
		out.writeInt(datlen);
		out.writeShort(VERSION);
		out.writeShort(numCol);
		
		/* Write col type */
		for(int i=0; i<numCol; i++)
			out.writeByte(enumType[i]);
		
		/* Nullness */
		byte[] nullBytes = boolArrayToByteArray(nullBits);
		out.write(nullBytes);
		
		/* Column Value */
		for(int i=0; i<numCol; i++) {
			if (!nullBits[i]) {
				/* Pad the alignment byte first */
				if (padLength[i] > 0) {
					out.write(padbytes, 0, padLength[i]);
				}

				/* Now, write the actual column value */
				switch(colType[i]) {
					case BIGINT:   out.writeLong(   ((Long)   colValue[i]).longValue());    break;
					case BOOLEAN:  out.writeBoolean(((Boolean)colValue[i]).booleanValue()); break;
					case FLOAT8:   out.writeDouble( ((Double) colValue[i]).doubleValue());  break;
					case INTEGER:  out.writeInt(    ((Integer)colValue[i]).intValue());     break;
					case REAL:     out.writeFloat(  ((Float)  colValue[i]).floatValue());   break;
					case SMALLINT: out.writeShort(  ((Short)  colValue[i]).shortValue());   break;
					/* For BYTEA format, add 4byte length header at the beginning  */
					case BYTEA:
						out.writeInt(((byte[])colValue[i]).length);
						out.write((byte[])colValue[i]);
						break;
					/* For text format, add 4byte length header (length include the "\0" at the end)
					 * at the beginning and add a "\0" at the end */
					default: {
						String outStr = (String)colValue[i]+"\0";
						out.writeInt(outStr.length());
						byte[] data = (outStr).getBytes(CHARSET);
						out.write(data);
						break;
					}
				}	
			}
		}
		
		/* End padding */
		out.write(padbytes, 0, endpadding);
	}

	/**
	 * Private helper to convert boolean array to byte array
	 */
	private static byte[] boolArrayToByteArray(boolean[] data) {
	    int len = data.length;
	    byte[] byts = new byte[getNullByteArraySize(len)];

	    for (int i = 0, j = 0, k = 7; i < data.length; i++) {
	        byts[j] |= (data[i] ? 1 : 0) << k--;
	        if (k < 0) { j++; k = 7; }
	    }
	    return byts;
	}
	
	/**
	 * Private helper to determine the size of the null byte array
	 */
	private static int getNullByteArraySize(int colCnt) {
		return (colCnt/8) + (colCnt%8 != 0 ? 1:0);
	}

	/**
	 * Private helper to convert byte array to boolean array
	 */
	private static boolean[] byteArrayToBooleanArray(byte[] data, int colCnt) {
	    boolean[] bools = new boolean[colCnt];
	    for (int i = 0, j = 0, k = 7; i < bools.length; i++) {
	        bools[i] = ((data[j] >> k--) & 0x01) == 1;
	        if (k < 0) { j++; k = 7; }
	    }
	    return bools;
	}
	
	/**
	 * Private helper to round up alignment for the given length
	 */
	private static int roundUpAlignment(int len, int align) {
		return (((len) + ((align) - 1)) & ~((align) - 1));
	}

	/**
	 * Return the byte form
	 * @return the binary representation of this object
	 * @throws if an I/O error occured
	 */
	public byte[] toBytes() throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
		DataOutputStream      dos  = new DataOutputStream(baos);
		
		this.write(dos);
		
		return baos.toByteArray();
	}
	
	/**
	 * Getter/Setter methods to get/set the column value
	 */
	
	/**
	 * Set the column value of the record
	 * @param colIdx the column index
	 * @param val the value
	 * @throws TypeMismatchException the column type does not match
	 */
	public void setLong(int colIdx, Long val)
	throws TypeMismatchException {
		checkType(BIGINT, colIdx, true);
		colValue[colIdx] = val;
	}

	/**
	 * Set the column value of the record
	 * @param colIdx the column index
	 * @param val the value
	 * @throws TypeMismatchException the column type does not match
	 */
	public void setBoolean(int colIdx, Boolean val)
	throws TypeMismatchException {
		checkType(BOOLEAN, colIdx, true);
		colValue[colIdx] = val;
	}

	/**
	 * Set the column value of the record
	 * @param colIdx the column index
	 * @param val the value
	 * @throws TypeMismatchException the column type does not match
	 */
	public void setBytes(int colIdx, byte[] val)
	throws TypeMismatchException {
		checkType(BYTEA, colIdx, true);
		colValue[colIdx] = val;
	}

	/**
	 * Set the column value of the record
	 * @param colIdx the column index
	 * @param val the value
	 * @throws TypeMismatchException the column type does not match
	 */
	public void setString(int colIdx, String val)
	throws TypeMismatchException {
		checkType(TEXT, colIdx, true);
		colValue[colIdx] = val;
	}

	/**
	 * Set the column value of the record
	 * @param colIdx the column index
	 * @param val the value
	 * @throws TypeMismatchException the column type does not match
	 */
	public void setFloat(int colIdx, Float val)
	throws TypeMismatchException {
		checkType(REAL, colIdx, true);
		colValue[colIdx] = val;
	}

	/**
	 * Set the column value of the record
	 * @param colIdx the column index
	 * @param val the value
	 * @throws TypeMismatchException the column type does not match
	 */
	public void setDouble(int colIdx, Double val)
	throws TypeMismatchException {
		checkType(FLOAT8, colIdx, true);
		colValue[colIdx] = val;
	}

	/**
	 * Set the column value of the record
	 * @param colIdx the column index
	 * @param val the value
	 * @throws TypeMismatchException the column type does not match
	 */
	public void setInt(int colIdx, Integer val)
	throws TypeMismatchException {
		checkType(INTEGER, colIdx, true);
		colValue[colIdx] = val;
	}

	/**
	 * Set the column value of the record
	 * @param colIdx the column index
	 * @param val the value
	 * @throws TypeMismatchException the column type does not match
	 */
	public void setShort(int colIdx, Short val)
	throws TypeMismatchException {
		checkType(SMALLINT, colIdx, true);
		colValue[colIdx] = val;
	}
	
	/**
	 * Get the column value of the record
	 * @param colIdx the column index
	 * @throws TypeMismatchException the column type does not match
	 */
	public Long getLong(int colIdx)
	throws TypeMismatchException {
		checkType(BIGINT, colIdx, false);
		return (Long)colValue[colIdx];
	}

	/**
	 * Get the column value of the record
	 * @param colIdx the column index
	 * @throws TypeMismatchException the column type does not match
	 */
	public Boolean getBoolean(int colIdx)
	throws TypeMismatchException {
		checkType(BOOLEAN, colIdx, false);
		return (Boolean)colValue[colIdx];
	}

	/**
	 * Get the column value of the record
	 * @param colIdx the column index
	 * @throws TypeMismatchException the column type does not match
	 */
	public byte[] getBytes(int colIdx)
	throws TypeMismatchException {
		checkType(BYTEA, colIdx, false);
		return (byte[])colValue[colIdx];
	}

	/**
	 * Get the column value of the record
	 * @param colIdx the column index
	 * @throws TypeMismatchException the column type does not match
	 */
	public String getString(int colIdx)
	throws TypeMismatchException {
		checkType(TEXT, colIdx, false);
		return (String)colValue[colIdx];
	}

	/**
	 * Get the column value of the record
	 * @param colIdx the column index
	 * @throws TypeMismatchException the column type does not match
	 */
	public Float getFloat(int colIdx)
	throws TypeMismatchException {
		checkType(REAL, colIdx, false);
		return (Float)colValue[colIdx];
	}

	/**
	 * Get the column value of the record
	 * @param colIdx the column index
	 * @throws TypeMismatchException the column type does not match
	 */
	public Double getDouble(int colIdx)
	throws TypeMismatchException {
		checkType(FLOAT8, colIdx, false);
		return (Double)colValue[colIdx];
	}
	
	/**
	 * Get the column value of the record
	 * @param colIdx the column index
	 * @throws TypeMismatchException the column type does not match
	 */
	public Integer getInt(int colIdx)
	throws TypeMismatchException {
		checkType(INTEGER, colIdx, false);
		return (Integer)colValue[colIdx];
	}
	
	/**
	 * Get the column value of the record
	 * @param colIdx the column index
	 * @throws TypeMismatchException the column type does not match
	 */
	public Short getShort(int colIdx)
	throws TypeMismatchException {
		checkType(SMALLINT, colIdx, false);
		return (Short)colValue[colIdx];
	}
	
	/**
	 * Return a string representation of the object
	 */
	public String toString() {
		if (colType == null)
			return null;
		
		String result= "";
		for(int i=0; i<colType.length; i++) {
			result += "Column " + i + ":";
			if (colValue[i] != null) {
				if (colType[i] == BYTEA)
					result += byteArrayInString((byte[])colValue[i]);
				else
					result += colValue[i].toString();
			}
			result += "\n";
		}		
		return result;
	}
	
	/**
	 * Helper printing function
	 */
	private static String byteArrayInString(byte[] data) {
		String result = "";
		for(int i=0; i<data.length; i++) {
			Byte b = new Byte(data[i]);
			result += b.intValue() + " ";
		}
		return result;
	}

	
	/**
	 * Private Helper to check the type mismatch
	 * If the expected type is stored as string, then it must be set
	 * via setString.
	 * Otherwise, the type must match.
	 */
	private void checkType(int inTyp, int idx, boolean isSet)
	throws TypeMismatchException {
		if (idx < 0 || idx >= colType.length)
			throw new TypeMismatchException(
					"Column index is out of range");
		
		int exTyp = colType[idx];
		String errmsg;

		if (isTextForm(exTyp)) {
			if (inTyp != TEXT)
				throw new TypeMismatchException(formErrorMsg(inTyp, TEXT, isSet));
		} else if (inTyp != exTyp)
			throw new TypeMismatchException(formErrorMsg(inTyp, exTyp, isSet));
	}
	
	private String formErrorMsg(int inTyp, int colTyp, boolean isSet) {
		String errmsg;
		if (isSet)
			errmsg = "Cannot set "+getTypeName(inTyp) + " to a " +
				getTypeName(colTyp) + " column";
		else
			errmsg = "Cannot get "+getTypeName(inTyp) + " from a " +
				getTypeName(colTyp) + " column";
		return errmsg;
	}

	/**
	 * Private Helper routine to tell whether a type is Text form or not
	 * @param type the type OID that we want to check
	 */
	private boolean isTextForm(int type) {
		if (type==BIGINT ||
			type==BOOLEAN ||
			type==BYTEA ||
			type==FLOAT8 ||
			type==INTEGER ||
			type==REAL ||
			type==SMALLINT)
			return false;
		return true;
	}
	
	/**
	 * Private Helper to get the type name for rasing error.
	 * If a given oid is not in the commonly used list, we
	 * would expect a TEXT for it (for the error message).
	 */
	private static String getTypeName(int oid) {
		switch(oid) {
		case BOOLEAN  : return "BOOLEAN";
		case BYTEA    : return "BYTEA";
		case CHAR     : return "CHAR";
		case BIGINT   : return "BIGINT";
		case SMALLINT : return "SMALLINT";
		case INTEGER  : return "INTEGER";
		case TEXT     : return "TEXT";
		case REAL     : return "REAL";
		case FLOAT8   : return "FLOAT8";
		case BPCHAR   : return "BPCHAR";
		case VARCHAR  : return "VARCHAR";
		case DATE     : return "DATE";
		case TIME     : return "TIME";
		case TIMESTAMP: return "TIMESTAMP";
		case NUMERIC  : return "NUMERIC";
		default:        return "TEXT";
		}
	}

}
