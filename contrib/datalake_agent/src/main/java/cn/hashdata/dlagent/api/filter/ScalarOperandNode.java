package cn.hashdata.dlagent.api.filter;

import cn.hashdata.dlagent.api.io.DataType;

import static java.util.Objects.requireNonNull;

/**
 * Represents a scalar value (String, Long, Int).
 */
public class ScalarOperandNode extends OperandNode {

    private final String value;

    /**
     * Constructs an ScalarOperandNode with the datum data type and value
     *
     * @param dataType the data type
     * @param value    the value
     */
    public ScalarOperandNode(DataType dataType, String value) {
        super(dataType);
        this.value = requireNonNull(value, "value is null");
    }

    /**
     * Returns the value of the scalar
     *
     * @return the value of the scalar
     */
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
