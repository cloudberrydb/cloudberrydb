package cn.hashdata.dlagent.api.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static cn.hashdata.dlagent.api.filter.Operator.*;

/**
 * Base tree pruner that maintains tree correctness by analyzing the current node and its children
 * after both the node and the children have been visited and potentially pruned.
 */
public abstract class BaseTreePruner implements TreeVisitor {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Override
    public Node before(Node node, int level) {
        return node;
    }

    @Override
    public abstract Node visit(Node node, int level);

    @Override
    public Node after(Node node, int level) {
        if (node instanceof OperatorNode) {
            OperatorNode operatorNode = (OperatorNode) node;
            Operator operator = operatorNode.getOperator();
            int childCount = operatorNode.childCount();
            if (AND == operator && childCount == 1) {
                Node promoted = Optional.ofNullable(operatorNode.getLeft()).orElse(operatorNode.getRight());
                LOG.debug("Child {} was promoted higher in the tree", promoted);
                // AND needs at least two children. If the operator has a single child node left,
                // we promote the child one level up the tree
                return promoted;
            } else if (OR == operator && childCount <= 1) {
                LOG.debug("Child with operator {} will be pruned because it has {} children", operator, childCount);
                operatorNode.setLeft(null);
                operatorNode.setRight(null);
                // OR needs two or more children
                return null;
            } else if ((AND == operator || NOT == operator) && childCount == 0) {
                LOG.debug("Child with operator {} will be pruned because it has no children", operator);
                // AND needs 2 children / NOT needs 1 child
                return null;
            }
        }
        return node;
    }
}
