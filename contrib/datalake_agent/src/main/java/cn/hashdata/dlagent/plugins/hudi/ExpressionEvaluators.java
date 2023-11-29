/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.hashdata.dlagent.plugins.hudi;

import cn.hashdata.dlagent.api.error.UnsupportedTypeException;
import cn.hashdata.dlagent.api.filter.CollectionOperandNode;
import cn.hashdata.dlagent.api.filter.ColumnIndexOperandNode;
import cn.hashdata.dlagent.api.filter.Node;
import cn.hashdata.dlagent.api.filter.OperandNode;
import cn.hashdata.dlagent.api.filter.Operator;
import cn.hashdata.dlagent.api.filter.OperatorNode;
import cn.hashdata.dlagent.api.filter.ScalarOperandNode;
import cn.hashdata.dlagent.api.filter.TreeVisitor;
import cn.hashdata.dlagent.api.utilities.ColumnDescriptor;
import cn.hashdata.dlagent.api.io.DataType;
import cn.hashdata.dlagent.api.utilities.Utilities;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.common.util.collection.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

public class ExpressionEvaluators implements TreeVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(ExpressionEvaluators.class);
  private final List<ColumnDescriptor> columnDescriptors;
  private final Deque<Evaluator> filterQueue;
  private final Set<String> refColumns;

  public ExpressionEvaluators(List<ColumnDescriptor> tupleDescription) {
    this.columnDescriptors = tupleDescription;
    this.filterQueue = new LinkedList<>();
    this.refColumns = new HashSet<>();
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

  private void processSimpleColumnOperator(OperatorNode operatorNode) {
    Operator operator = operatorNode.getOperator();
    ColumnIndexOperandNode columnIndexOperand = operatorNode.getColumnIndexOperand();
    OperandNode valueOperandNode = operatorNode.getValueOperand();

    ColumnDescriptor filterColumn = columnDescriptors.get(columnIndexOperand.index());
    String colName = filterColumn.columnName();
    Object colValue;
    Type.TypeID dataType;

    refColumns.add(colName);

    if (valueOperandNode instanceof CollectionOperandNode) {
      CollectionOperandNode collectionOperand = (CollectionOperandNode) valueOperandNode;
      dataType = convertDataType(collectionOperand.getDataType());
      colValue = collectionOperand
              .getData()
              .stream()
              .map(data -> Utilities.boxLiteral(Utilities.convertDataValue(
                      collectionOperand.getDataType().getTypeElem(),
                      data)))
              .collect(Collectors.toList());
    } else {
      ScalarOperandNode scalarOperand = (ScalarOperandNode) valueOperandNode;
      dataType = convertDataType(scalarOperand.getDataType());
      colValue = Utilities.boxLiteral(Utilities.convertDataValue(
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

    Evaluator expression;
    switch (operator) {
      case LESS_THAN:
        expression = LessThan.getInstance().bindVal(new ValueLiteralExpression(colValue))
                .bindFieldReference(new FieldReferenceExpression(dataType, colName));
        break;
      case GREATER_THAN:
        expression = GreaterThan.getInstance().bindVal(new ValueLiteralExpression(colValue))
                .bindFieldReference(new FieldReferenceExpression(dataType, colName));
        break;
      case LESS_THAN_OR_EQUAL:
        expression = LessThanOrEqual.getInstance().bindVal(new ValueLiteralExpression(colValue))
                .bindFieldReference(new FieldReferenceExpression(dataType, colName));
        break;
      case GREATER_THAN_OR_EQUAL:
        expression = GreaterThanOrEqual.getInstance().bindVal(new ValueLiteralExpression(colValue))
                .bindFieldReference(new FieldReferenceExpression(dataType, colName));
        break;
      case EQUALS:
        expression = EqualTo.getInstance().bindVal(new ValueLiteralExpression(colValue))
                .bindFieldReference(new FieldReferenceExpression(dataType, colName));
        break;
      case NOT_EQUALS:
        expression = NotEqualTo.getInstance().bindVal(new ValueLiteralExpression(colValue))
                .bindFieldReference(new FieldReferenceExpression(dataType, colName));
        break;
      case IS_NULL:
        expression = IsNull.getInstance().bindFieldReference(new FieldReferenceExpression(dataType, colName));
        break;
      case IS_NOT_NULL:
        expression = IsNotNull.getInstance().bindFieldReference(new FieldReferenceExpression(dataType, colName));
        break;
      case IN:
        if (colValue instanceof Collection) {
          @SuppressWarnings("unchecked")
          Collection<Object> l = (Collection<Object>) colValue;
          In in = In.getInstance();
          FieldReferenceExpression rExpr = new FieldReferenceExpression(dataType, colName);
          in.bindFieldReference(rExpr);
          in.bindVals(l);
          expression = in;
        } else {
          throw new IllegalArgumentException("value should be instance of List for IN operation");
        }
        break;
      default:
        throw new UnsupportedOperationException(String.format("Filter push-down is not supported for %s operation", operator));
    }

    filterQueue.push(expression);
  }

  private void processLogicalOperator(Operator operator) {
    Evaluator right = filterQueue.poll();
    Evaluator left = null;

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
        And andEval = And.getInstance();
        filterQueue.push(andEval.bindEvaluator(left, right));
        break;
      case OR:
        Or orEval = Or.getInstance();
        filterQueue.push(orEval.bindEvaluator(left, right));
        break;
      case NOT:
        Not notEval = Not.getInstance();
        filterQueue.push(notEval.bindEvaluator(right));
    }
  }

  private Type.TypeID convertDataType(DataType dataType) {
    switch (dataType) {
      case BIGINT:
        return Type.TypeID.LONG;
      case INTEGER:
      case SMALLINT:
        return Type.TypeID.INT;
      case REAL:
        return Type.TypeID.FLOAT;
      case NUMERIC:
        return Type.TypeID.DECIMAL;
      case FLOAT8:
        return Type.TypeID.DOUBLE;
      case TEXT:
      case VARCHAR:
      case BPCHAR:
        return Type.TypeID.STRING;
      case BOOLEAN:
        return Type.TypeID.BOOLEAN;
      case DATE:
        return Type.TypeID.DATE;
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP:
        return Type.TypeID.TIMESTAMP;
      default:
        throw new UnsupportedTypeException(String.format("DataType %s unsupported", dataType));
    }
  }

  public Pair<Evaluator, Set<String>> build() {
    if (filterQueue.isEmpty()) {
      return null;
    }

    Evaluator expression = filterQueue.poll();
    if (!filterQueue.isEmpty()) {
      throw new IllegalStateException("Filter queue is not empty after visiting all nodes");
    }

    return Pair.of(expression, refColumns);
  }

  /**
   * Decides whether it's possible to match based on the column values and column stats.
   * The evaluator can be nested.
   */
  public interface Evaluator extends Serializable {

    /**
     * Evaluates whether it's possible to match based on the column stats.
     *
     * @param columnStatsMap column statistics
     * @return false if there is no any possible to match, true otherwise.
     */
    boolean eval(Map<String, ColumnStats> columnStatsMap);
  }

  /**
   * Leaf evaluator which depends on the given field.
   */
  public abstract static class LeafEvaluator implements Evaluator {

    // referenced field type
    protected Type.TypeID type;

    // referenced field name
    protected String name;

    public LeafEvaluator bindFieldReference(FieldReferenceExpression expr) {
      this.type = expr.getDataType();
      this.name = expr.getName();
      return this;
    }

    protected ColumnStats getColumnStats(Map<String, ColumnStats> columnStatsMap) {
      ColumnStats columnStats = columnStatsMap.get(this.name);
      ValidationUtils.checkState(
          columnStats != null,
          "Can not find column " + this.name);
      return columnStats;
    }
  }

  /**
   * Leaf evaluator which compares the field value with literal values.
   */
  public abstract static class NullFalseEvaluator extends LeafEvaluator {

    // the constant literal value
    protected Object val;

    public NullFalseEvaluator bindVal(ValueLiteralExpression vExpr) {
      this.val = vExpr.getValue();
      return this;
    }

    @Override
    public final boolean eval(Map<String, ColumnStats> columnStatsMap) {
      if (this.val == null) {
        return false;
      } else {
        return eval(this.val, getColumnStats(columnStatsMap), this.type);
      }
    }

    protected abstract boolean eval(@NotNull Object val, ColumnStats columnStats, Type.TypeID type);
  }

  /**
   * To evaluate = expr.
   */
  public static class EqualTo extends NullFalseEvaluator {
    public static EqualTo getInstance() {
      return new EqualTo();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, Type.TypeID type) {
      Object minVal = columnStats.getMinVal();
      Object maxVal = columnStats.getMaxVal();
      if (minVal == null || maxVal == null) {
        return false;
      }
      if (compare(minVal, val, type) > 0) {
        return false;
      }
      return compare(maxVal, val, type) >= 0;
    }
  }

  /**
   * To evaluate <> expr.
   */
  public static class NotEqualTo extends NullFalseEvaluator {
    public static NotEqualTo getInstance() {
      return new NotEqualTo();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, Type.TypeID type) {
      Object minVal = columnStats.getMinVal();
      Object maxVal = columnStats.getMaxVal();
      if (minVal == null || maxVal == null) {
        return false;
      }
      // return false if min == max == val, otherwise return true.
      // because the bounds are not necessarily a min or max value, this cannot be answered using them.
      // notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
      return compare(minVal, val, type) != 0 || compare(maxVal, val, type) != 0;
    }
  }

  /**
   * To evaluate IS NULL expr.
   */
  public static class IsNull extends LeafEvaluator {
    public static IsNull getInstance() {
      return new IsNull();
    }

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      ColumnStats columnStats = getColumnStats(columnStatsMap);
      return columnStats.getNullCnt() > 0;
    }
  }

  /**
   * To evaluate IS NOT NULL expr.
   */
  public static class IsNotNull extends LeafEvaluator {
    public static IsNotNull getInstance() {
      return new IsNotNull();
    }

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      ColumnStats columnStats = getColumnStats(columnStatsMap);
      // should consider FLOAT/DOUBLE & NAN
      return columnStats.getMinVal() != null || columnStats.getNullCnt() <= 0;
    }
  }

  /**
   * To evaluate < expr.
   */
  public static class LessThan extends NullFalseEvaluator {
    public static LessThan getInstance() {
      return new LessThan();
    }

    @Override
    public boolean eval(@NotNull Object val, ColumnStats columnStats, Type.TypeID type) {
      Object minVal = columnStats.getMinVal();
      if (minVal == null) {
        return false;
      }
      return compare(minVal, val, type) < 0;
    }
  }

  /**
   * To evaluate > expr.
   */
  public static class GreaterThan extends NullFalseEvaluator {
    public static GreaterThan getInstance() {
      return new GreaterThan();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, Type.TypeID type) {
      Object maxVal = columnStats.getMaxVal();
      if (maxVal == null) {
        return false;
      }
      return compare(maxVal, val, type) > 0;
    }
  }

  /**
   * To evaluate <= expr.
   */
  public static class LessThanOrEqual extends NullFalseEvaluator {
    public static LessThanOrEqual getInstance() {
      return new LessThanOrEqual();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, Type.TypeID type) {
      Object minVal = columnStats.getMinVal();
      if (minVal == null) {
        return false;
      }
      return compare(minVal, val, type) <= 0;
    }
  }

  /**
   * To evaluate >= expr.
   */
  public static class GreaterThanOrEqual extends NullFalseEvaluator {
    public static GreaterThanOrEqual getInstance() {
      return new GreaterThanOrEqual();
    }

    @Override
    protected boolean eval(@NotNull Object val, ColumnStats columnStats, Type.TypeID type) {
      Object maxVal = columnStats.getMaxVal();
      if (maxVal == null) {
        return false;
      }
      return compare(maxVal, val, type) >= 0;
    }
  }

  /**
   * To evaluate IN expr.
   */
  public static class In extends LeafEvaluator {
    private static final Logger LOGGER = LoggerFactory.getLogger(In.class);

    private static final int IN_PREDICATE_LIMIT = 200;

    public static In getInstance() {
      return new In();
    }

    private Object[] vals;

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      if (Arrays.stream(vals).anyMatch(Objects::isNull)) {
        return false;
      }
      ColumnStats columnStats = getColumnStats(columnStatsMap);
      Object minVal = columnStats.getMinVal();
      Object maxVal = columnStats.getMaxVal();
      if (minVal == null) {
        return false; // values are all null and literalSet cannot contain null.
      }

      if (vals.length > IN_PREDICATE_LIMIT) {
        // skip evaluating the predicate if the number of values is too big
        LOGGER.warn("Skip evaluating in predicate because the number of values is too big!");
        return true;
      }

      return Arrays.stream(vals).anyMatch(v ->
          compare(minVal, v, this.type) <= 0 && compare(maxVal, v, this.type) >= 0);
    }

    public void bindVals(Object... vals) {
      this.vals = vals;
    }
  }

  /**
   * A special evaluator which is not possible to match any condition.
   */
  public static class AlwaysFalse implements Evaluator {
    private static final long serialVersionUID = 1L;

    public static AlwaysFalse getInstance() {
      return new AlwaysFalse();
    }

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      return false;
    }
  }

  // component predicate

  /**
   * To evaluate NOT expr.
   */
  public static class Not implements Evaluator {
    private static final long serialVersionUID = 1L;

    public static Not getInstance() {
      return new Not();
    }

    private Evaluator evaluator;

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      return !this.evaluator.eval(columnStatsMap);
    }

    public Evaluator bindEvaluator(Evaluator evaluator) {
      this.evaluator = evaluator;
      return this;
    }
  }

  /**
   * To evaluate AND expr.
   */
  public static class And implements Evaluator {
    private static final long serialVersionUID = 1L;

    public static And getInstance() {
      return new And();
    }

    private Evaluator[] evaluators;

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      for (Evaluator evaluator : evaluators) {
        if (!evaluator.eval(columnStatsMap)) {
          return false;
        }
      }
      return true;
    }

    public Evaluator bindEvaluator(Evaluator... evaluators) {
      this.evaluators = evaluators;
      return this;
    }
  }

  /**
   * To evaluate OR expr.
   */
  public static class Or implements Evaluator {
    private static final long serialVersionUID = 1L;

    public static Or getInstance() {
      return new Or();
    }

    private Evaluator[] evaluators;

    @Override
    public boolean eval(Map<String, ColumnStats> columnStatsMap) {
      for (Evaluator evaluator : evaluators) {
        if (evaluator.eval(columnStatsMap)) {
          return true;
        }
      }
      return false;
    }

    public Evaluator bindEvaluator(Evaluator... evaluators) {
      this.evaluators = evaluators;
      return this;
    }
  }

  public static class FieldReferenceExpression {
    private Type.TypeID type;
    private String name;

    FieldReferenceExpression(Type.TypeID dataType, String name) {
      this.type = dataType;
      this.name = name;
    }
    public Type.TypeID getDataType() {
      return this.type;
    }

    public String getName() {
      return this.name;
    }
  }

  public static class ValueLiteralExpression {
    private Object value;

    ValueLiteralExpression(Object value) {
      this.value = value;
    }

    public Object getValue() {
      return this.value;
    }
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private static int compare(@NotNull Object val1, @NotNull Object val2, Type.TypeID logicalType) {
    switch (logicalType) {
      case TIME:
      case TIMESTAMP:
      case LONG:
      case DATE:
        return ((Long) val1).compareTo((Long) val2);
      case BOOLEAN:
        return ((Boolean) val1).compareTo((Boolean) val2);
      case INT:
        return ((Integer) val1).compareTo((Integer) val2);
      case FLOAT:
        return ((Float) val1).compareTo((Float) val2);
      case DOUBLE:
        return ((Double) val1).compareTo((Double) val2);
      case FIXED:
      case BINARY:
      case UUID:
        return compareBytes((byte[]) val1, (byte[]) val2);
      case STRING:
        return ((String) val1).compareTo((String) val2);
      case DECIMAL:
        return ((BigDecimal) val1).compareTo((BigDecimal) val2);
      default:
        throw new UnsupportedOperationException("Unsupported type: " + logicalType);
    }
  }

  private static int compareBytes(byte[] v1, byte[] v2) {
    int len1 = v1.length;
    int len2 = v2.length;
    int lim = Math.min(len1, len2);

    int k = 0;
    while (k < lim) {
      byte c1 = v1[k];
      byte c2 = v2[k];
      if (c1 != c2) {
        return c1 - c2;
      }
      k++;
    }
    return len1 - len2;
  }
}
