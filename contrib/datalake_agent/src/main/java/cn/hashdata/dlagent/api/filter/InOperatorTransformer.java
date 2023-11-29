package cn.hashdata.dlagent.api.filter;

import cn.hashdata.dlagent.api.io.DataType;

import java.util.List;

/**
 * Transforms IN operator into a chain of OR operators. This transformer is
 * useful for predicate builders that do not support the IN operator.
 */
public class InOperatorTransformer implements TreeVisitor {

    /**
     * {@inheritDoc}
     */
    @Override
    public Node before(Node node, int level) {
        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node visit(Node node, int level) {

        if (node instanceof OperatorNode) {

            OperatorNode operatorNode = (OperatorNode) node;

            if (operatorNode.getOperator() == Operator.IN
                    && operatorNode.getLeft() instanceof ColumnIndexOperandNode
                    && operatorNode.getRight() instanceof CollectionOperandNode) {

                ColumnIndexOperandNode columnNode = (ColumnIndexOperandNode) operatorNode.getLeft();
                CollectionOperandNode collectionOperandNode = (CollectionOperandNode) operatorNode.getRight();
                List<String> data = collectionOperandNode.getData();
                DataType type = collectionOperandNode.getDataType().getTypeElem() != null
                        ? collectionOperandNode.getDataType().getTypeElem()
                        : collectionOperandNode.getDataType();

                // Transform the IN operator into a chain of ORs
                //       IN
                //        |
                //    --------
                //    |      |
                //   _1_   11,12
                //
                //  The transformed branch will look like this:

                //                   OR
                //                    |
                //         ------------------------
                //         |                      |
                //         eq                     eq
                //         |                      |
                //     ---------              ---------
                //     |       |              |       |
                //    _1_      11            _1_      12

                // build the first node as the equal operator of the column and the scalar operand
                Node currentNode = new OperatorNode(Operator.EQUALS, columnNode, new ScalarOperandNode(type, data.get(0)));

                for (int i = 1; i < data.size(); i++) {
                    // current node becomes left node
                    // scalar becomes right node
                    // the or operator becomes the current node
                    Node rightNode = new OperatorNode(Operator.EQUALS, columnNode, new ScalarOperandNode(type, data.get(i)));
                    currentNode = new OperatorNode(Operator.OR, currentNode, rightNode);
                }

                return currentNode;
            }
        }

        return node;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node after(Node node, int level) {
        return node;
    }
}
