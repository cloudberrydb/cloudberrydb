package cn.hashdata.dlagent.api.filter;

import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;

import java.util.EnumSet;
import java.util.List;

/**
 * A tree pruner that removes operator nodes for non-supported Greenplum column data types.
 */
public class SupportedDataTypePruner extends BaseTreePruner {

    private final List<ColumnDescriptor> columnDescriptors;
    private final EnumSet<DataType> supportedDataTypes;

    /**
     * Constructor
     *
     * @param columnDescriptors  the list of column descriptors for the table
     * @param supportedDataTypes the EnumSet of supported data types
     */
    public SupportedDataTypePruner(List<ColumnDescriptor> columnDescriptors,
                                   EnumSet<DataType> supportedDataTypes) {
        this.columnDescriptors = columnDescriptors;
        this.supportedDataTypes = supportedDataTypes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node visit(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            if (!operatorNode.getOperator().isLogical()) {
                ColumnDescriptor columnDescriptor = columnDescriptors.get(operatorNode.getColumnIndexOperand().index());
                DataType datatype = columnDescriptor.getDataType();
                if (!supportedDataTypes.contains(datatype)) {
                    // prune the operator node if its operand is a column of unsupported type
                    LOG.debug("DataType oid={} for column=(index:{} name:{}) is not supported",
                            datatype.getOID(), columnDescriptor.columnIndex(), columnDescriptor.columnName());
                    return null;
                }
            }
        }
        return node;
    }
}
