package cn.hashdata.dlagent.plugins.iceberg;

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
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class IcebergExpressionBuilder implements TreeVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergExpressionBuilder.class);
    private final List<ColumnDescriptor> columnDescriptors;
    private final Deque<Expression> filterQueue;

    public IcebergExpressionBuilder(List<ColumnDescriptor> tupleDescription) {
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
        String filterColumnName = filterColumn.columnName();
        Object filterValue;

        if (valueOperandNode instanceof CollectionOperandNode) {
            CollectionOperandNode collectionOperand = (CollectionOperandNode) valueOperandNode;

            filterValue = collectionOperand
                    .getData()
                    .stream()
                    .map(data -> Utilities.boxLiteral(Utilities.convertDataValue(
                            collectionOperand.getDataType(),
                            data)))
                    .collect(Collectors.toList());
        } else {
            ScalarOperandNode scalarOperand = (ScalarOperandNode) valueOperandNode;
            filterValue = Utilities.boxLiteral(Utilities.convertDataValue(
                    scalarOperand.getDataType(),
                    scalarOperand.getValue()));
        }

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

        Expression expression;
        switch (operator) {
            case LESS_THAN:
                expression = Expressions.lessThan(filterColumnName, filterValue);
                break;
            case GREATER_THAN:
                expression = Expressions.greaterThan(filterColumnName, filterValue);
                break;
            case LESS_THAN_OR_EQUAL:
                expression = Expressions.lessThanOrEqual(filterColumnName, filterValue);
                break;
            case GREATER_THAN_OR_EQUAL:
                expression = Expressions.greaterThanOrEqual(filterColumnName, filterValue);
                break;
            case EQUALS:
                expression = Expressions.equal(filterColumnName, filterValue);
                break;
            case NOT_EQUALS:
                expression = Expressions.notEqual(filterColumnName, filterValue);
                break;
            case IS_NULL:
                expression = Expressions.isNull(filterColumnName);
                break;
            case IS_NOT_NULL:
                expression = Expressions.notNull(filterColumnName);
                break;
            case IN:
                if (filterValue instanceof Collection) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> l = (Collection<Object>) filterValue;
                    expression = Expressions.in(filterColumnName, l);
                } else {
                    throw new IllegalArgumentException("filterValue should be instance of List for IN operation");
                }
                break;
            default:
                throw new UnsupportedOperationException(String.format("Filter push-down is not supported for %s operation", operator));
        }

        filterQueue.push(expression);
    }

    private void processLogicalOperator(Operator operator) {
        Expression right = filterQueue.poll();
        Expression left = null;

        if (right == null) {
            throw new IllegalStateException("Unable to process logical operator " + operator.toString());
        }

        if (operator == Operator.AND || operator == Operator.OR) {
            left = filterQueue.poll();

            if (left == null) {
                throw new IllegalStateException("Unable to process logical operator " + operator.toString());
            }
        }

        switch (operator) {
            case AND:
                filterQueue.push(Expressions.and(left, right));
                break;
            case OR:
                filterQueue.push(Expressions.or(left, right));
                break;
            case NOT:
                filterQueue.push(Expressions.not(right));
                break;
        }
    }

    public Expression build() {
        if (filterQueue.isEmpty()) {
            return null;
        }

        Expression expression = filterQueue.poll();
        if (!filterQueue.isEmpty()) {
            throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
        }

        return expression;
    }
}
