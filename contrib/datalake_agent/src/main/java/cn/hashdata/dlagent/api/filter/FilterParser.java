package cn.hashdata.dlagent.api.filter;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import cn.hashdata.dlagent.api.io.DataType;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Stack;

/**
 * The parser code which goes over a filter string and pushes operands onto a
 * stack. Once an operation is read, a {@link Node} is created with two pop-ed
 * operands.
 * <br>
 * The filter string is of the pattern:
 * [attcode][attnum][constcode][constval][constsizecode][constsize][constdata][constvalue][opercode][opernum]
 * <br>
 * A sample string of filters looks like this:
 * <code>a2c23s1d5o1a1c25s3dabco2o7</code>
 * which means {@code column#2 < 5 AND column#1 > "abc"}
 * <br>
 * It is a RPN serialized representation of a filters tree in GPDB where
 * <ul>
 * <li>a means an attribute (column)</li>
 * <li>c means a constant followed by the datatype oid</li>
 * <li>s means the length of the data in bytes</li>
 * <li>d denotes the start of the constant data</li>
 * <li>o means operator</li>
 * <li>l means logical operator</li>
 * </ul>
 * <br>
 * For constants all three portions are required in order to parse the data
 * type, the length of the data in bytes and the data itself
 * <br>
 * The parsing operation parses each element of the filter (constants, columns,
 * operations) and adds them to a stack. When the parser sees an op code 'o' or
 * 'l' it pops off two elements from the stack assigns them as children of the
 * op and pushes itself onto the stack. After parsing is complete there should
 * only be one element in the stack, the root node of the filter's tree
 * representation which is returned from this method
 * <br>
 * FilterParser only knows about columns and constants and it produces an
 * expression tree.
 * FilterParser makes sure column objects are always on the left of the
 * expression (when relevant).
 */
public class FilterParser {
    private int index;
    private byte[] filterByteArr;
    private final Stack<Node> operandsStack;
    private static final char COL_OP = 'a';
    private static final char SCALAR_CONST_OP = 'c';
    private static final char LIST_CONST_OP = 'm';
    private static final char CONST_LEN = 's';
    private static final char CONST_DATA = 'd';
    private static final char COMP_OP = 'o';
    private static final char LOG_OP = 'l';

    public static final String DEFAULT_CHARSET = "UTF-8";

    private static final EnumSet<DataType> SUPPORTED_DATA_TYPES = EnumSet.of(
            DataType.BIGINT,
            DataType.INTEGER,
            DataType.SMALLINT,
            DataType.REAL,
            DataType.NUMERIC,
            DataType.FLOAT8,
            DataType.TEXT,
            DataType.VARCHAR,
            DataType.BPCHAR,
            DataType.BOOLEAN,
            DataType.DATE,
            DataType.TIMESTAMP,
            DataType.TIMESTAMP_WITH_TIME_ZONE,
            DataType.TIME,
            DataType.BYTEA
    );

    private static final Operator[] OPERATOR_ARRAY = new Operator[]{
            Operator.NOOP,
            Operator.LESS_THAN,
            Operator.GREATER_THAN,
            Operator.LESS_THAN_OR_EQUAL,
            Operator.GREATER_THAN_OR_EQUAL,
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.LIKE,
            Operator.IS_NULL,
            Operator.IS_NOT_NULL,
            Operator.IN
    };

    private static final Operator[] LOGICAL_OPERATOR_ARRAY = new Operator[]{
            Operator.AND,
            Operator.OR,
            Operator.NOT
    };

    /**
     * Thrown when a filter's parsing exception occurs.
     */
    class FilterStringSyntaxException extends IOException {
        FilterStringSyntaxException(String desc) {
            super(String.format("%s (%s)", desc, filterByteArr != null ?
                    "filter string: '" + new String(filterByteArr) + "'" : "null filter string"));
        }
    }

    /**
     * Constructs a FilterParser.
     */
    public FilterParser() {
        operandsStack = new Stack<>();
    }

    /**
     * Parses the filter string
     *
     * @param filterString the filter string to parse
     * @return the parsed filter
     * @throws FilterStringSyntaxException  if the filter string was invalid
     * @throws UnsupportedEncodingException if the named charset is not supported
     */
    public Node parse(String filterString)
            throws FilterStringSyntaxException, UnsupportedEncodingException {

        if (filterString == null) {
            throw new FilterStringSyntaxException("filter parsing ended with no result");
        }

        return parse(filterString.getBytes(DEFAULT_CHARSET));
    }

    /**
     * Parses the string filter.
     *
     * @param filter the filter to parse
     * @return the parsed filter
     * @throws FilterStringSyntaxException  if the filter string had wrong syntax
     * @throws UnsupportedEncodingException if the named charset is not supported
     */
    private Node parse(byte[] filter)
            throws FilterStringSyntaxException, UnsupportedEncodingException {
        index = 0;
        filterByteArr = filter;

        while (index < filterByteArr.length) {
            char op = (char) filterByteArr[index++];
            switch (op) {
                case COL_OP:
                    operandsStack.push(parseColumnIndex());
                    break;
                case SCALAR_CONST_OP:
                    operandsStack.push(parseScalarParameter());
                    break;
                case LIST_CONST_OP:
                    operandsStack.push(parseListParameter());
                    break;
                case COMP_OP:
                    operandsStack.push(parseComparisonOperator());
                    break;
                // Handle parsing logical operator (HAWQ-964)
                case LOG_OP:
                    operandsStack.push(parseLogicalOperator());
                    break;
                default:
                    index--; // move index back to operand location
                    throw new FilterStringSyntaxException(String.format("unknown opcode %s(%d) at %d", op, (int) op, index));
            }
        }

        if (operandsStack.empty()) {
            throw new FilterStringSyntaxException("filter parsing ended with no result");
        }

        Node result = operandsStack.pop();

        if (!operandsStack.empty()) {
            throw new FilterStringSyntaxException("Stack not empty, missing operators?");
        }

        if (result instanceof OperandNode) {
            throw new FilterStringSyntaxException("filter parsing failed, missing operators?");
        }

        return result;
    }

    /**
     * Safely converts a long value to an int.
     *
     * @param value the long value to convert
     * @return the converted int value
     * @throws FilterStringSyntaxException if the long value is not inside an int scope
     */
    private int safeLongToInt(Long value) throws FilterStringSyntaxException {
        if (value > Integer.MAX_VALUE || value < Integer.MIN_VALUE) {
            throw new FilterStringSyntaxException("value " + value + " larger than intmax ending at " + index);
        }

        return value.intValue();
    }

    private int parseConstDataType() throws FilterStringSyntaxException {
        if (!Character.isDigit((char) filterByteArr[index])) {
            throw new FilterStringSyntaxException("datatype OID should follow at " + index);
        }

        String digits = parseDigits();

        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            throw new FilterStringSyntaxException("invalid numeric argument at " + digits);
        }
    }

    private int parseDataLength() throws FilterStringSyntaxException {
        if (((char) filterByteArr[index]) != CONST_LEN) {
            throw new FilterStringSyntaxException("data length delimiter 's' expected at " + index);
        }

        index++;
        return parseInt();
    }

    private int parseInt() throws FilterStringSyntaxException {
        if (index == filterByteArr.length) {
            throw new FilterStringSyntaxException("numeric argument expected at " + index);
        }

        String digits = parseDigits();

        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            throw new FilterStringSyntaxException("invalid numeric argument " + digits);
        }
    }

    private String convertDataType(byte[] byteData, int start, int end, DataType dataType)
            throws FilterStringSyntaxException, UnsupportedEncodingException {

        if (byteData.length < end)
            throw new FilterStringSyntaxException("filter string is shorter than expected");

        if (!SUPPORTED_DATA_TYPES.contains(dataType))
            throw new FilterStringSyntaxException(
                    String.format("DataType %s unsupported", dataType));

        return new String(byteData, start, end - start, DEFAULT_CHARSET);
    }

    /**
     * Parses the column index operand
     *
     * @return a {@link ColumnIndexOperandNode}
     * @throws FilterStringSyntaxException when parsing fails
     */
    private Node parseColumnIndex() throws FilterStringSyntaxException {
        return new ColumnIndexOperandNode(safeLongToInt(parseNumber()));
    }

    /**
     * Parses either a number or a string.
     */
    private ScalarOperandNode parseScalarParameter()
            throws FilterStringSyntaxException, UnsupportedEncodingException {
        validateNotEndOfArray();

        DataType dataType = DataType.get(parseConstDataType());
        validateDataType(dataType);

        int dataLength = parseDataLength();
        validateDataLengthAndType(dataLength);
        index++;

        String data = convertDataType(filterByteArr, index, index + dataLength, dataType);
        index += dataLength;

        return new ScalarOperandNode(dataType, data);
    }

    /**
     * Parses parameters with a list of values.
     *
     * @return the parsed {@link CollectionOperandNode}
     * @throws FilterStringSyntaxException  when parsing fails
     * @throws UnsupportedEncodingException if the named charset is not supported
     */
    private CollectionOperandNode parseListParameter()
            throws FilterStringSyntaxException, UnsupportedEncodingException {
        validateNotEndOfArray();

        DataType dataType = DataType.get(parseConstDataType());
        validateDataType(dataType);

        List<String> data = new ArrayList<>();

        if (dataType.getTypeElem() == null) {
            throw new FilterStringSyntaxException("expected non-scalar datatype, but got datatype with oid = " + dataType.getOID());
        }

        while (((char) filterByteArr[index]) == CONST_LEN) {
            int dataLength = parseDataLength();
            validateDataLengthAndType(dataLength);

            index++;
            data.add(convertDataType(filterByteArr, index, index + dataLength, dataType.getTypeElem()));
            index += dataLength;
        }

        return new CollectionOperandNode(dataType, data);
    }

    /**
     * Parses the comparison operator
     *
     * @return the parsed {@link Node}
     * @throws FilterStringSyntaxException when parsing fails
     */
    private Node parseComparisonOperator() throws FilterStringSyntaxException {
        int opNumber = safeLongToInt(parseNumber());
        Operator operator = getOperatorFromArray(OPERATOR_ARRAY, opNumber);
        if (operator == null) {
            throw new FilterStringSyntaxException("unknown op ending at " + index);
        }

        // Pop right operand
        if (operandsStack.empty()) {
            throw new FilterStringSyntaxException("missing operands for op " + operator + " at " + index);
        }
        Node rightOperand = operandsStack.pop();

        // all operations other than null checks require 2 operands
        Node result;
        if (operator == Operator.IS_NULL || operator == Operator.IS_NOT_NULL) {
            result = new OperatorNode(operator, rightOperand);
        } else {
            // Pop left operand
            if (operandsStack.empty()) {
                throw new FilterStringSyntaxException("missing operands for op " + operator + " at " + index);
            }
            Node leftOperand = operandsStack.pop();

            if (!(leftOperand instanceof OperandNode) || !(rightOperand instanceof OperandNode)) {
                throw new FilterStringSyntaxException(String.format("missing logical operator before op %s at %d", operator.name(), index));
            }

            // Normalize order, evaluate
            // Column should be on the left
            result = (leftOperand instanceof ScalarOperandNode)
                    // column on the right, reverse expression
                    ? new OperatorNode(operator.transpose(), rightOperand, leftOperand)
                    // no swap, column on the left
                    : new OperatorNode(operator, leftOperand, rightOperand);
        }
        // Store result on stack
        return result;
    }

    /**
     * Parses the logical operator
     *
     * @return the parsed {@link Node}
     * @throws FilterStringSyntaxException when parsing fails
     */
    private Node parseLogicalOperator() throws FilterStringSyntaxException {
        int opNumber = safeLongToInt(parseNumber());
        Operator operator = getOperatorFromArray(LOGICAL_OPERATOR_ARRAY, opNumber);

        if (operator == null) {
            throw new FilterStringSyntaxException("unknown op ending at " + index);
        }

        Node result;
        if (operator == Operator.NOT) {
            Node exp = operandsStack.pop();
            result = new OperatorNode(operator, exp);
        } else if (operator == Operator.AND || operator == Operator.OR) {
            Node rightOperand = operandsStack.pop();
            Node leftOperand = operandsStack.pop();

            if (!(rightOperand instanceof OperatorNode) || !(leftOperand instanceof OperatorNode)) {
                throw new FilterStringSyntaxException(String.format("logical operator %s expects two operator nodes at %d", operator, index));
            }

            result = new OperatorNode(operator, leftOperand, rightOperand);
        } else {
            throw new FilterStringSyntaxException("unknown logical op code " + opNumber);
        }
        return result;
    }

    private Long parseNumber() throws FilterStringSyntaxException {
        if (index == filterByteArr.length) {
            throw new FilterStringSyntaxException("numeric argument expected at " + index);
        }

        String digits = parseDigits();

        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            throw new FilterStringSyntaxException("invalid numeric argument " + digits);
        }

    }

    /**
     * Returns the operator from the given array if the array is not null
     * and the index is within the bounds of the array, null otherwise
     *
     * @param array the OperatorNode array
     * @param index the index
     * @return the operator if exists, null otherwise
     */
    private Operator getOperatorFromArray(Operator[] array, int index) {
        return (array != null && index >= 0 && index < array.length) ? array[index] : null;
    }

    /*
     * Parses the longest sequence of digits into a number
     * advances the index accordingly
     */
    private String parseDigits() throws FilterStringSyntaxException {
        String result;
        int i = index;
        int filterLength = filterByteArr.length;

        // allow sign
        if (filterLength > 0) {
            char chr = (char) filterByteArr[i];
            if (chr == '-' || chr == '+') {
                ++i;
            }
        }
        for (; i < filterLength; ++i) {
            char chr = (char) filterByteArr[i];
            if (chr < '0' || chr > '9') {
                break;
            }
        }

        if (i == index) {
            throw new FilterStringSyntaxException("numeric argument expected at " + index);
        }

        result = new String(filterByteArr, index, i - index);
        index = i;
        return result;
    }

    /**
     * Ensures that the data length and type of the element being parsed is valid
     *
     * @param dataLength the length of the data to be parsed
     * @throws FilterStringSyntaxException the exception describing if the length or type are invalid
     */
    private void validateDataLengthAndType(int dataLength) throws FilterStringSyntaxException {
        if (index + dataLength > filterByteArr.length) {
            throw new FilterStringSyntaxException("data size larger than filter string starting at " + index);
        }

        if (((char) filterByteArr[index]) != CONST_DATA) {
            throw new FilterStringSyntaxException("data delimiter 'd' expected at " + index);
        }
    }

    /**
     * Validates that the current parsing index is not at the end of the byte array
     *
     * @throws FilterStringSyntaxException the exception when the index is at the end of the byte array
     */
    private void validateNotEndOfArray() throws FilterStringSyntaxException {
        if (index == filterByteArr.length) {
            throw new FilterStringSyntaxException("argument should follow at " + index);
        }
    }

    /**
     * Validates that the data type is supported
     *
     * @param dataType the parsed data type
     * @throws FilterStringSyntaxException the exception when the data type is not supported
     */
    private void validateDataType(DataType dataType) throws FilterStringSyntaxException {
        if (dataType == DataType.UNSUPPORTED_TYPE) {
            throw new FilterStringSyntaxException("invalid DataType OID at " + (index - 1));
        }
    }
}
