package cn.hashdata.dlagent.api.filter;

/**
 * Supported operations by the parser.
 */
public enum Operator {
    NOOP(null, false) {
        @Override
        public String toString() {
            throw new UnsupportedOperationException("NOOP doesn't have an operator");
        }
    },
    LESS_THAN("<", false) {
        @Override
        public Operator transpose() {
            return GREATER_THAN;
        }
    },
    GREATER_THAN(">", false) {
        @Override
        public Operator transpose() {
            return LESS_THAN;
        }
    },
    LESS_THAN_OR_EQUAL("<=", false) {
        @Override
        public Operator transpose() {
            return GREATER_THAN_OR_EQUAL;
        }
    },
    GREATER_THAN_OR_EQUAL(">=", false) {
        @Override
        public Operator transpose() {
            return LESS_THAN_OR_EQUAL;
        }
    },
    EQUALS("=", false),
    NOT_EQUALS("<>", false),
    LIKE("LIKE", false),
    IS_NULL("IS NULL", false),
    IS_NOT_NULL("IS NOT NULL", false),
    IN("IN", false),
    AND("AND", true),
    OR("OR", true),
    NOT("NOT", true);

    private final String printableName;
    private final boolean isLogical;

    Operator(String printableName, boolean isLogical) {
        this.printableName = printableName;
        this.isLogical = isLogical;
    }

    public boolean isLogical() {
        return isLogical;
    }

    /**
     * Transposes operator
     * e.g. > turns into <, = turns into =
     *
     * @return the transposed operator
     */
    public Operator transpose() {
        return this;
    }

    @Override
    public String toString() {
        return printableName;
    }
}
