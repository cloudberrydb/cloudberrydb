package cn.hashdata.dlagent.api.filter;


import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Traverses the expression tree
 */
public class TreeTraverser {

    private static final Logger LOG = LoggerFactory.getLogger(TreeTraverser.class);

    /**
     * In order depth-first traversal L-M-R. This method will visit the node
     * with each of the provided visitors in the order they are provided.
     *
     * @param node     the node
     * @param visitors one or more visitors interface implementation
     * @return the traversed node
     */
    public Node traverse(Node node, TreeVisitor... visitors) {

        if (ArrayUtils.isEmpty(visitors)) {
            throw new IllegalArgumentException("You need to provide at least one visitor for this traverser");
        }

        Node result = node;
        for (TreeVisitor visitor : visitors) {
            result = traverse(result, visitor, 0);
        }
        return result;
    }

    /**
     * In order depth-first traversal L-M-R
     *
     * @param node    the node
     * @param visitor the visitor interface implementation
     * @return the traversed node
     */
    protected Node traverse(Node node, TreeVisitor visitor, final int level) {
        if (node == null) return null;

        node = visitor.before(node, level);
        // Traverse the left node
        traverseHelper(node, 0, visitor, level + 1);
        // Visit this node
        node = visitor.visit(node, level);
        // Traverse the right node
        traverseHelper(node, 1, visitor, level + 1);
        node = visitor.after(node, level);

        return node;
    }

    /*
     * This method helps during the traversing of a node. When the index is 0,
     * we process the left node, and when the index is 1 we process the right
     * node. This method also helps with the pruning of nodes and promoting,
     * a child node one level up.
     */
    private void traverseHelper(Node node, int index, TreeVisitor visitor, final int level) {
        if (node == null) return;
        Node child = index == 0 ? node.getLeft() : node.getRight();
        Node processed = traverse(child, visitor, level);

        if (processed == child) {
            return;
        }

        String debugMessage;

        if (processed == null) {
            debugMessage = "{} child {} was pruned {}";
        } else {

            // This happens when AND operation end up with a single
            // child. For example:
            //                          AND
            //                           |
            //               ------------------------
            //               |                      |
            //              AND                     >
            //               |                      |
            //        ----------------          ---------
            //        |              |          |       |
            //        >              <         _2_     1200
            //        |              |
            //    --------       --------
            //    |      |       |      |
            //   _1_     5      _1_     10
            //
            // If only the AND and > operators are supported, the right
            // branch of the second AND ( _1_ < 10 ) will be dropped and
            // the left branch will be promoted up in the tree. The
            // resulting tree will look like this:
            //                         AND
            //                          |
            //               ------------------------
            //               |                      |
            //               >                      >
            //               |                      |
            //           ---------              ---------
            //           |       |              |       |
            //          _1_      5             _2_     1200

            debugMessage = "{} child {} was pruned, and child {} was promoted higher in the tree";
        }

        if (index == 0) {
            LOG.debug(debugMessage, "Left", child, processed);
            node.setLeft(processed);
        } else {
            LOG.debug(debugMessage, "Right", child, processed);
            node.setRight(processed);
        }
    }
}
