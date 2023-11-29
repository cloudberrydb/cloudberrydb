package cn.hashdata.dlagent.api.filter;

/**
 * A node in the expression tree
 */
public class Node {

    private Node left;
    private Node right;

    /**
     * Default constructor
     */
    public Node() {
        this(null, null);
    }

    /**
     * Constructs a node with a left Node
     *
     * @param left the left node
     */
    public Node(Node left) {
        this(left, null);
    }

    /**
     * Constructs a node with a left and right node
     *
     * @param left  the left node
     * @param right the right node
     */
    public Node(Node left, Node right) {
        this.left = left;
        this.right = right;
    }

    /**
     * Sets the left {@link Node} of the tree
     *
     * @param left the left node
     */
    public void setLeft(Node left) {
        this.left = left;
    }

    /**
     * Returns the left {@link Node}
     *
     * @return the left {@link Node}
     */
    public Node getLeft() {
        return left;
    }

    /**
     * Sets the right {@link Node} of the tree
     *
     * @param right the right node
     */
    public void setRight(Node right) {
        this.right = right;
    }

    /**
     * Returns the right {@link Node}
     *
     * @return the right {@link Node}
     */
    public Node getRight() {
        return right;
    }

    /**
     * Returns the number of children for this node
     *
     * @return the number of children for this node
     */
    public int childCount() {
        int count = 0;
        if (left != null) count++;
        if (right != null) count++;
        return count;
    }
}
