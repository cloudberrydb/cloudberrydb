package cn.hashdata.dlagent.api.filter;

/**
 * Tree visitor interface
 */
public interface TreeVisitor {

    /**
     * Called right before a Node is visited
     *
     * @param node  the Node that will be visited next
     * @param level the level in the recursion
     * @return the resulting node from the visit
     */
    Node before(Node node, final int level);

    /**
     * Called during the visit of a Node
     *
     * @param node  the Node being visited
     * @param level the level in the recursion
     * @return the resulting node from the visit
     */
    Node visit(Node node, final int level);

    /**
     * Called right after the Node has been visited
     *
     * @param node  the Node that completed the visit
     * @param level the level in the recursion
     * @return the resulting node from the visit
     */
    Node after(Node node, final int level);

}
