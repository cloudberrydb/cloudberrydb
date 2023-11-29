package cn.hashdata.dlagent.api.filter;

/**
 * Represents a column index.
 */
public class ColumnIndexOperandNode extends OperandNode {

    private final int index;

    /**
     * Constructs a ColumnIndexOperandNode with the column index
     *
     * @param index the zero-based column index
     */
    public ColumnIndexOperandNode(int index) {
        super(null);
        this.index = index;
    }

    /**
     * Returns the column index
     *
     * @return the column index
     */
    public int index() {
        return index;
    }

    @Override
    public String toString() {
        return String.format("_%d_", index);
    }
}
