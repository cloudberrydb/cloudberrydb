package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.error.DlRuntimeException;
import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.filter.CollectionOperandNode;
import cn.hashdata.dlagent.api.filter.ColumnIndexOperandNode;
import cn.hashdata.dlagent.api.filter.Node;
import cn.hashdata.dlagent.api.filter.OperandNode;
import cn.hashdata.dlagent.api.filter.Operator;
import cn.hashdata.dlagent.api.filter.OperatorNode;
import cn.hashdata.dlagent.api.filter.ScalarOperandNode;
import cn.hashdata.dlagent.api.filter.TreeVisitor;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import cn.hashdata.dlagent.api.utilities.Utilities;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Deque;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Stack;
import java.util.stream.Collectors;

public class HudiPartitionFilterBuilder implements TreeVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(HudiPartitionFilterBuilder.class);
    private final List<ColumnDescriptor> columnDescriptors;
    private final Deque<ExprNodeGenericFuncDesc> filterQueue;
    private final String tableName;

    public HudiPartitionFilterBuilder(String tableName,
                                      List<ColumnDescriptor> tupleDescription) {
        this.tableName = tableName;
        this.columnDescriptors = tupleDescription;
        this.filterQueue = new LinkedList<>();
    }

    @Override
    public Node before(Node node, final int level) { return node; }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (!operator.isLogical()) {
                processSimpleColumnOperator(operatorNode);
            }
        }
        return node;
    }

    @Override
    public Node after(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (operator.isLogical()) {
                processLogicalOperator(operator);
            }
        }
        return node;
    }

    /**
     * Builds a single argument
     *
     * @param operatorNode the operatorNode node
     * @return true if the argument is build, false otherwise
     */
    private void processSimpleColumnOperator(OperatorNode operatorNode) {
        Operator operator = operatorNode.getOperator();
        ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
        OperandNode valueOperandNode = operatorNode.getValueOperand();

        ColumnDescriptor filterColumn = columnDescriptors.get(columnIndexOperand.index());
        String colName = filterColumn.columnName();
        Object colValue;
        PrimitiveTypeInfo hiveType;
        if (valueOperandNode instanceof CollectionOperandNode) {
            CollectionOperandNode collectionOperand = (CollectionOperandNode) valueOperandNode;

            hiveType = convertDataType(collectionOperand.getDataType());
            colValue = collectionOperand
                    .getData()
                    .stream()
                    .map(data -> Utilities.boxLiteral(Utilities.convertDataValue(
                            collectionOperand.getDataType(),
                            data)))
                    .collect(Collectors.toList());
        } else {
            ScalarOperandNode scalarOperand = (ScalarOperandNode) valueOperandNode;

            hiveType = convertDataType(scalarOperand.getDataType());
            colValue = Utilities.boxLiteral(Utilities.convertDataValue(
                    scalarOperand.getDataType(),
                    scalarOperand.getValue()));
        }

        ExprBuilder exprBuilder = new ExprBuilder(tableName);

        if (operator == Operator.NOOP) {
            // NOT boolean wraps a NOOP
            //       NOT
            //        |
            //       NOOP
            //        |
            //    ---------
            //   |         |
            //   4        true
            // that needs to be replaced with equals

            // also IN
            operator = Operator.EQUALS;
        }

        ExprNodeGenericFuncDesc expression;
        switch (operator) {
            case LESS_THAN:
                expression = exprBuilder.col(hiveType, colName).val(hiveType, colValue).pred("<", 2).build();
                break;
            case GREATER_THAN:
                expression = exprBuilder.col(hiveType, colName).val(hiveType, colValue).pred(">", 2).build();
                break;
            case LESS_THAN_OR_EQUAL:
                expression = exprBuilder.col(hiveType, colName).val(hiveType, colValue).pred("<=", 2).build();
                break;
            case GREATER_THAN_OR_EQUAL:
                expression = exprBuilder.col(hiveType, colName).val(hiveType, colValue).pred(">=", 2).build();
                break;
            case EQUALS:
                expression = exprBuilder.col(hiveType, colName).val(hiveType, colValue).pred("=", 2).build();
                break;
            case NOT_EQUALS:
                expression = exprBuilder.col(hiveType, colName).val(hiveType, colValue).pred("!=", 2).build();
                break;
            case IS_NULL:
                expression = exprBuilder.col(hiveType, colName).val(hiveType, "NULL").pred("=", 2).build();
                break;
            case IS_NOT_NULL:
                expression = exprBuilder.col(hiveType, colName).val(hiveType, "NULL").pred("!=", 2).build();
                break;
            default:
                throw new UnsupportedOperationException(String.format("Filter push-down is not supported for %s operation", operator));
        }

        filterQueue.push(expression);
    }

    private ExprNodeGenericFuncDesc getCompoundExpr(List<ExprNodeDesc> args, String op) {
        ExprNodeGenericFuncDesc compoundExpr;
        try {
            compoundExpr = ExprNodeGenericFuncDesc.newInstance(
                    FunctionRegistry.getFunctionInfo(op).getGenericUDF(), args);
        } catch (SemanticException e) {
            throw new IllegalStateException("Convert to Hive expr failed. Error: " + e.getMessage());
        }
        return compoundExpr;
    }

    private void processLogicalOperator(Operator operator) {
        ExprNodeGenericFuncDesc right = filterQueue.poll();
        ExprNodeGenericFuncDesc left = null;

        if (right == null) {
            throw new IllegalStateException("Unable to process logical operator " + operator.toString());
        }

        if (operator == Operator.AND || operator == Operator.OR) {
            left = filterQueue.poll();

            if (left == null) {
                throw new IllegalStateException("Unable to process logical operator " + operator.toString());
            }
        }

        List<ExprNodeDesc> args = new ArrayList<>();
        switch (operator) {
            case AND:
                args.add(left);
                args.add(right);
                filterQueue.push(getCompoundExpr(args, "and"));
                break;
            case OR:
                args.add(left);
                args.add(right);
                filterQueue.push(getCompoundExpr(args, "or"));
                break;
            case NOT:
                args.add(right);
                filterQueue.push(getCompoundExpr(args, "not"));
        }
    }

    /**
     * Convert from GPDB column type to Hive column type
     *
     * @param dataType the data type
     * @return hive primitive type info
     */
    private PrimitiveTypeInfo convertDataType(DataType dataType) {
        switch (dataType) {
            case BIGINT:
                return TypeInfoFactory.longTypeInfo;
            case INTEGER:
                return TypeInfoFactory.intTypeInfo;
            case SMALLINT:
                return TypeInfoFactory.shortTypeInfo;
            case REAL:
                return TypeInfoFactory.floatTypeInfo;
            case NUMERIC:
                return TypeInfoFactory.decimalTypeInfo;
            case FLOAT8:
                return TypeInfoFactory.doubleTypeInfo;
            case TEXT:
            case VARCHAR:
                return TypeInfoFactory.varcharTypeInfo;
            case BPCHAR:
                return TypeInfoFactory.charTypeInfo;
            case BOOLEAN:
                return TypeInfoFactory.booleanTypeInfo;
            case DATE:
                return TypeInfoFactory.dateTypeInfo;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP:
                return TypeInfoFactory.timestampTypeInfo;
            default:
                throw new UnsupportedTypeException(String.format("DataType %s unsupported", dataType));
        }
    }

    public ExprNodeGenericFuncDesc build() {
        if (filterQueue.isEmpty()) {
            return null;
        }

        ExprNodeGenericFuncDesc expression = filterQueue.poll();
        if (!filterQueue.isEmpty()) {
            throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
        }

        return expression;
    }

    public static class ExprBuilder {
        private final String tableName;
        private final Stack<ExprNodeDesc> stack = new Stack<>();

        public ExprBuilder(String tableName) {
            this.tableName = tableName;
        }

        public ExprNodeGenericFuncDesc build() {
            if (stack.size() != 1) {
                throw new IllegalStateException("Build Hive expression Failed: " + stack.size());
            }
            return (ExprNodeGenericFuncDesc) stack.pop();
        }

        public ExprBuilder pred(String name, int args) {
            return fn(name, TypeInfoFactory.booleanTypeInfo, args);
        }

        private ExprBuilder fn(String name, TypeInfo ti, int args) {
            List<ExprNodeDesc> children = new ArrayList<>();
            for (int i = 0; i < args; ++i) {
                children.add(stack.pop());
            }
            try {
                stack.push(new ExprNodeGenericFuncDesc(ti,
                        FunctionRegistry.getFunctionInfo(name).getGenericUDF(), children));
            } catch (SemanticException e) {
                throw new DlRuntimeException("Build Hive expression Failed. Error: " + e.getMessage());
            }
            return this;
        }

        public ExprBuilder col(TypeInfo ti, String col) {
            stack.push(new ExprNodeColumnDesc(ti, col, tableName, true));
            return this;
        }

        public ExprBuilder val(TypeInfo ti, Object val) {
            stack.push(new ExprNodeConstantDesc(ti, val));
            return this;
        }
    }
}
