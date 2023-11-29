package cn.hashdata.dlagent.api.filter;

/**
 * OperatorNode node (i.e. AND, OR, =, etc.)
 */
public class OperatorNode extends Node {

    private final Operator operator;

    /**
     * Constructs a new {@link OperatorNode} with a left operand
     *
     * @param operator    the operator
     * @param leftOperand the left operand
     */
    public OperatorNode(Operator operator, Node leftOperand) {
        this(operator, leftOperand, null);
    }

    /**
     * Constructs a new {@link OperatorNode} with left and right operands
     *
     * @param operator     the operator
     * @param leftOperand  the left operand
     * @param rightOperand the right operand
     */
    public OperatorNode(Operator operator, Node leftOperand, Node rightOperand) {
        super(leftOperand, rightOperand);
        this.operator = operator;
    }

    /**
     * Returns the operator
     *
     * @return the operator
     */
    public Operator getOperator() {
        return operator;
    }

    /**
     * Returns the {@link ColumnIndexOperandNode} for this {@link OperatorNode}
     *
     * @return the {@link ColumnIndexOperandNode} for this {@link OperatorNode}
     */
    public ColumnIndexOperandNode getColumnIndexOperand() {
        Node left = getLeft();
        if (!(left instanceof ColumnIndexOperandNode)) {
            throw new IllegalArgumentException(String.format(
                    "Operator %s does not contain a column index operand", operator));
        }
        return (ColumnIndexOperandNode) left;
    }

    /**
     * Returns the {@link OperandNode} for this {@link OperatorNode}
     *
     * @return the {@link OperandNode} for this {@link OperatorNode}
     */
    public OperandNode getValueOperand() {
        Node right = getRight();
        if (right instanceof ScalarOperandNode || right instanceof CollectionOperandNode) {
            return (OperandNode) right;
        }
        return null;
    }
}
