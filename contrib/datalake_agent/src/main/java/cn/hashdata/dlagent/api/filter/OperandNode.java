package cn.hashdata.dlagent.api.filter;

import cn.hashdata.dlagent.api.io.DataType;

/**
 * Scalar, Column Index, List
 */
public class OperandNode extends Node {

    private final DataType dataType;

    /**
     * Constructs an OperandNode with the given data type
     *
     * @param dataType the data type
     */
    public OperandNode(DataType dataType) {
        this.dataType = dataType;
    }

    /**
     * Returns the data type of the operand
     *
     * @return the data type of the operand
     */
    public DataType getDataType() {
        return dataType;
    }
}
