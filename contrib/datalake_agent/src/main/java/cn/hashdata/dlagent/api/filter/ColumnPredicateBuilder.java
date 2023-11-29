package cn.hashdata.dlagent.api.filter;

import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A class that generates predicates with the provided column names
 */
public class ColumnPredicateBuilder extends ToStringTreeVisitor {

    private final String quoteString;
    private final List<ColumnDescriptor> columnDescriptors;

    /**
     * Stores the index of the last processed column
     */
    protected int lastIndex;

    /**
     * Constructor that takes a list of column descriptors
     *
     * @param columnDescriptors the list of column descriptors
     */
    public ColumnPredicateBuilder(List<ColumnDescriptor> columnDescriptors) {
        this("", columnDescriptors);
    }

    /**
     * Constructor that takes the quote string and a list of column descriptors
     *
     * @param quoteString       the quote string
     * @param columnDescriptors the list of column descriptors
     */
    public ColumnPredicateBuilder(String quoteString,
                                  List<ColumnDescriptor> columnDescriptors) {
        this.quoteString = quoteString;
        this.columnDescriptors = columnDescriptors;
    }

    @Override
    protected String getNodeValue(OperandNode operandNode) {
        if (operandNode instanceof ColumnIndexOperandNode) {
            ColumnIndexOperandNode columnIndexOperand = (ColumnIndexOperandNode) operandNode;

            /* We need the column index (column is guaranteed to be on the left,
             * so it always comes first. The column index is needed to get the
             * column type. The column type information is required to determine
             * how the value will be processed
             */
            lastIndex = columnIndexOperand.index();
            ColumnDescriptor columnDescriptor = columnDescriptors.get(lastIndex);
            return String.format("%s%s%s", quoteString, columnDescriptor.columnName(), quoteString);
        }

        // Obtain the datatype of the column
        ColumnDescriptor columnDescriptor = columnDescriptors.get(lastIndex);
        DataType type = columnDescriptor.getDataType();

        if (operandNode instanceof CollectionOperandNode) {
            CollectionOperandNode collectionOperand = (CollectionOperandNode) operandNode;
            String listValue = collectionOperand.getData().stream()
                    .map(s -> serializeValue(type, s))
                    .collect(Collectors.joining(","));
            return String.format("(%s)", listValue);
        } else {
            String value = super.getNodeValue(operandNode);
            return serializeValue(type, value);
        }
    }

    /**
     * Serializes the value
     *
     * @param type  the value type
     * @param value the value
     * @return the serialized value
     */
    protected String serializeValue(DataType type, String value) {
        return value;
    }

    /**
     * Returns the list of {@link ColumnDescriptor}
     *
     * @return the list of {@link ColumnDescriptor}
     */
    protected List<ColumnDescriptor> getColumnDescriptors() {
        return columnDescriptors;
    }
}
