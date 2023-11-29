package cn.hashdata.dlagent.api.filter;

import java.util.EnumSet;

/**
 * A tree pruner that removes operator nodes for non-supported operators.
 */
public class SupportedOperatorPruner extends BaseTreePruner {

    private final EnumSet<Operator> supportedOperators;

    /**
     * Constructor
     *
     * @param supportedOperators the set of supported operators
     */
    public SupportedOperatorPruner(EnumSet<Operator> supportedOperators) {
        this.supportedOperators = supportedOperators;
    }

    @Override
    public Node visit(Node node, final int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            if (!supportedOperators.contains(operator)) {
                // prune the operator node if its operator is not supported
                LOG.debug("Operator {} is not supported", operator);
                return null;
            }
        }
        return node;
    }
}
