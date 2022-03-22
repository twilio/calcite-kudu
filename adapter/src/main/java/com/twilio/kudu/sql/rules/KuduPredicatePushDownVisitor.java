/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.sql.rules;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.google.common.collect.Lists;
import com.twilio.kudu.sql.CalciteKuduPredicate;
import com.twilio.kudu.sql.CalciteKuduTable;
import com.twilio.kudu.sql.ComparisonPredicate;
import com.twilio.kudu.sql.InListPredicate;
import com.twilio.kudu.sql.NullPredicate;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexDynamicParam;

import java.util.Map;
import java.util.Optional;

import org.apache.calcite.rex.RexNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.kudu.client.KuduPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KuduPredicatePushDownVisitor returns a List of a List of
 * CalciteKuduPredicates. The inner list represents a
 * {@link org.apache.kudu.client.AsyncKuduScanner} that can be executed
 * independently of each other. The outer list therefore represents a List of
 * {@code AsyncKuduScanner}s that will satifisy the provided filters.
 *
 * It is expected that it is called with {@link RexNode} that represent the
 * filters
 */
public class KuduPredicatePushDownVisitor implements RexBiVisitor<List<List<CalciteKuduPredicate>>, RexCall> {
  static final Logger logger = LoggerFactory.getLogger(KuduPredicatePushDownVisitor.class);

  private boolean allExpressionsConverted = true;

  private final RexBuilder rexBuilder;
  private final int primaryKeyColumnCount;
  private final boolean useOrClause;

  public KuduPredicatePushDownVisitor(RexBuilder rexBuilder, int primaryKeyColumnCount, boolean useOrClause) {
    this.rexBuilder = rexBuilder;
    this.primaryKeyColumnCount = primaryKeyColumnCount;
    this.useOrClause = useOrClause;
  }

  /**
   * @return true if we can push down all filters to kudu
   */
  public boolean areAllFiltersApplied() {
    return allExpressionsConverted;
  }

  public static List<List<CalciteKuduPredicate>> mergePredicateLists(SqlKind booleanOp,
      List<List<CalciteKuduPredicate>> left, List<List<CalciteKuduPredicate>> right) {
    switch (booleanOp) {
    case AND:
      if (left.isEmpty()) {
        return right;
      }
      if (right.isEmpty()) {
        return left;
      }
      // Merge both left and right together into one list. Every predicate in left and
      // every predicate
      // in right must be applied in the scan.
      final ArrayList<List<CalciteKuduPredicate>> mergedPredicates = new ArrayList<>();
      for (List<CalciteKuduPredicate> leftPredicates : left) {
        for (List<CalciteKuduPredicate> rightPredicates : right) {
          List<CalciteKuduPredicate> innerList = new ArrayList<>();
          innerList.addAll(leftPredicates);
          innerList.addAll(rightPredicates);
          mergedPredicates.add(innerList);
        }
      }
      return mergedPredicates;
    case OR:
      // If there is no predicates in Left or Right, that means we are unable to push
      // down
      // the entire scan tokens into Kudu. We cannot apply the right either in that
      // case
      // so need to create an empty scan.
      if (left.isEmpty()) {
        return Collections.emptyList();
      }
      if (right.isEmpty()) {
        return Collections.emptyList();
      }

      // In this case, we have a set of predicates on the left, that will map to one
      // set of KuduScanTokens.
      // And we have a set of predicates on the right, that too will map to it's own
      // set
      // of KuduScanTokens
      final ArrayList<List<CalciteKuduPredicate>> combined = new ArrayList<>();
      for (List<CalciteKuduPredicate> leftPredicates : left) {
        combined.add(leftPredicates);
      }
      for (List<CalciteKuduPredicate> rightPredicates : right) {
        combined.add(rightPredicates);
      }
      return combined;
    default:
      throw new IllegalArgumentException(
          String.format("Passed in a SqlKind operation that isn't supported: %s", booleanOp));
    }
  }

  private List<List<CalciteKuduPredicate>> mergeBoolean(SqlKind booleanOp, List<List<CalciteKuduPredicate>> left,
      List<List<CalciteKuduPredicate>> right) {
    // happens only on the reduce call at the start.
    if (left == null) {
      return right;
    }
    final List<List<CalciteKuduPredicate>> combined = mergePredicateLists(booleanOp, left, right);

    // If there is no predicates in Left or Right, that means we are unable to push
    // down
    // the entire scan tokens into Kudu.
    if (booleanOp == SqlKind.OR && combined.isEmpty()) {
      return setEmpty();
    }
    return combined;
  }

  /**
   * Converts a Sarg to SQL, generating "operand IN (c1, c2, ...)" if the ranges
   * are all points.
   *
   **/
  private <C extends Comparable<C>> List<List<CalciteKuduPredicate>> visitSearch(RexCall call, RexCall parent) {
    final RexLiteral literal = (RexLiteral) call.operands.get(1);
    final Sarg<C> sarg = literal.getValueAs(Sarg.class);
    int columnIndex = getColumnIndex(call.operands.get(0));
    // if we have an IN list on a primary key count do not use an IN list predicate,
    // instead use an OR clause which ends up generating a separate scan token for
    // each clause
    // this is required in case we are sorting by a part of the primary key
    // TODO see if there is a way to only force using an OR clause when sorting by
    // part of the
    // primary key
    if (sarg.isPoints() && !useOrClause) {
      final List<RexNode> inNodes = sarg.rangeSet.asRanges().stream()
          .map(range -> rexBuilder.makeLiteral(range.lowerEndpoint(), literal.getType(), true, true))
          .collect(Collectors.toList());
      // If there is only one value, use EQUAL instead of IN LIST.
      if (inNodes.size() == 1) {
        return Collections.singletonList(Collections.singletonList(new ComparisonPredicate(columnIndex,
            KuduPredicate.ComparisonOp.EQUAL, castLiteral((RexLiteral) inNodes.get(0)))));
      } else {
        List<Object> inList = inNodes.stream().map(node -> castLiteral((RexLiteral) node)).collect(Collectors.toList());
        return Collections.singletonList(Collections.singletonList(new InListPredicate(columnIndex, inList)));
      }
    } else {
      final RexNode condition = RexUtil.expandSearch(rexBuilder, null, call);
      return condition.accept(this, parent);
    }
  }

  /**
   * A sql function call, process it. Including handling boolean calls.
   *
   * @param call this is the relational call object to process
   *
   * @return updated list of kudu predicates
   */
  public List<List<CalciteKuduPredicate>> visitCall(RexCall call, RexCall parent) {
    final SqlKind callType = call.getOperator().getKind();

    switch (callType) {
    case SEARCH:
      return visitSearch(call, parent);
    case EQUALS:
    case PERIOD_EQUALS:
    case NOT_EQUALS: // Not equals will still return setEmpty() as findKuduOp method will return
                     // Optional.empty()
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      return call.operands.get(1).accept(this, call);
    case OR:
    case AND:
      return call.operands.stream().map(rexNode -> rexNode.accept(this, call)).reduce(null,
          (left, right) -> mergeBoolean(callType, left, right));
    case NOT:
      if (call.operands.get(0) instanceof RexInputRef) {
        RexInputRef falseColumn = (RexInputRef) call.operands.get(0);
        return Collections.singletonList(Collections.singletonList(
            new ComparisonPredicate(falseColumn.getIndex(), KuduPredicate.ComparisonOp.EQUAL, Boolean.FALSE)));
      }
    }
    return setEmpty();
  }

  private Optional<KuduPredicate.ComparisonOp> findKuduOp(RexCall functionCall) {
    final SqlKind callType = functionCall.getOperator().getKind();
    switch (callType) {
    case PERIOD_EQUALS:
    case EQUALS:
      return Optional.of(KuduPredicate.ComparisonOp.EQUAL);
    case GREATER_THAN:
      return Optional.of(KuduPredicate.ComparisonOp.GREATER);
    case GREATER_THAN_OR_EQUAL:
      return Optional.of(KuduPredicate.ComparisonOp.GREATER_EQUAL);
    case LESS_THAN:
      return Optional.of(KuduPredicate.ComparisonOp.LESS);
    case LESS_THAN_OR_EQUAL:
      return Optional.of(KuduPredicate.ComparisonOp.LESS_EQUAL);
    }
    return Optional.empty();
  }

  public List<List<CalciteKuduPredicate>> visitInputRef(RexInputRef inputRef, RexCall parent) {
    return Collections.singletonList(Collections
        .singletonList(new ComparisonPredicate(inputRef.getIndex(), KuduPredicate.ComparisonOp.EQUAL, Boolean.TRUE)));
  }

  public List<List<CalciteKuduPredicate>> visitLocalRef(RexLocalRef localRef, RexCall parent) {
    return setEmpty();
  }

  private int getColumnIndex(RexNode node) {
    if (node instanceof RexInputRef) {
      return ((RexInputRef) node).getIndex();
    }
    // if the node is an expression of the form expr#14=[CAST($t3):INTEGER] we need
    // to exptract
    // the column index from the input to the CAST expression
    else if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      if (call.getKind() == SqlKind.CAST) {
        return getColumnIndex(call.getOperands().get(0));
      }
    }
    throw new UnsupportedOperationException("Unable to determine column index from node " + node);
  }

  private Object castLiteral(RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
    case BOOLEAN:
      return RexLiteral.booleanValue(literal);
    case DECIMAL:
      return literal.getValueAs(BigDecimal.class);
    case DOUBLE:
      return literal.getValueAs(Double.class);
    case FLOAT:
      return literal.getValueAs(Float.class);
    case TIMESTAMP:
      // multiplied by 1000 as TIMESTAMP is in milliseconds and Kudu want's
      // microseconds.
      return literal.getValueAs(Long.class) * 1000;
    case CHAR:
    case VARCHAR:
      return literal.getValueAs(String.class);
    case TINYINT:
    case SMALLINT:
    case INTEGER:
      return literal.getValueAs(Integer.class);
    case BIGINT:
      return literal.getValueAs(Long.class);
    case BINARY:
      return (((ByteBuffer) literal.getValue4()).array());
    }
    throw new IllegalArgumentException(String.format("Unable to cast literal to Kudu: %s", literal));
  }

  private Long shiftTimestampByOneInterval(final Long epochMicros, final TimeUnitRange timeUnitRange,
      final boolean shiftForward) {
    final Instant currentInstant = Instant.ofEpochMilli(epochMicros / 1000L);
    switch (timeUnitRange) {
    case MINUTE:
      return (shiftForward) ? currentInstant.plus(1L, ChronoUnit.MINUTES).toEpochMilli() * 1000L
          : currentInstant.minus(1L, ChronoUnit.MINUTES).toEpochMilli() * 1000L;
    case HOUR:
      return (shiftForward) ? currentInstant.plus(1L, ChronoUnit.HOURS).toEpochMilli() * 1000L
          : currentInstant.minus(1L, ChronoUnit.HOURS).toEpochMilli() * 1000L;
    case DAY:
      return (shiftForward) ? currentInstant.plus(1L, ChronoUnit.DAYS).toEpochMilli() * 1000L
          : currentInstant.minus(1L, ChronoUnit.DAYS).toEpochMilli() * 1000L;
    case MONTH:
      return (shiftForward)
          ? ZonedDateTime.ofInstant(currentInstant, ZoneOffset.UTC).plus(1L, ChronoUnit.MONTHS).toInstant()
              .toEpochMilli() * 1000L
          : ZonedDateTime.ofInstant(currentInstant, ZoneOffset.UTC).minus(1L, ChronoUnit.MONTHS).toInstant()
              .toEpochMilli() * 1000L;
    case YEAR:
      return (shiftForward)
          ? ZonedDateTime.ofInstant(currentInstant, ZoneOffset.UTC).plus(1L, ChronoUnit.YEARS).toInstant()
              .toEpochMilli() * 1000L
          : ZonedDateTime.ofInstant(currentInstant, ZoneOffset.UTC).minus(1L, ChronoUnit.YEARS).toInstant()
              .toEpochMilli() * 1000L;
    default:
      throw new IllegalArgumentException(String.format("Unsupported timeUnitRange:%s", timeUnitRange.name()));
    }
  }

  /**
   * This visit method adds a predicate. this is the leaf of a tree so it gets to
   * create a fresh list of list
   */
  public List<List<CalciteKuduPredicate>> visitLiteral(RexLiteral literal, RexCall parent) {
    if (parent != null) {
      Optional<KuduPredicate.ComparisonOp> maybeOp = findKuduOp(parent);
      final RexNode left = parent.operands.get(0);

      if (left.getKind() == SqlKind.INPUT_REF) {
        final int index = getColumnIndex(left);

        if (literal.getType().getSqlTypeName() == SqlTypeName.NULL) {
          return Collections.singletonList(Collections.singletonList(new NullPredicate(index, false)));
        }
        // everything else requires op to be set.
        else if (maybeOp.isPresent()) {
          return Collections.singletonList(
              Collections.singletonList(new ComparisonPredicate(index, maybeOp.get(), castLiteral(literal))));
        } else if (!maybeOp.isPresent()) {
          return setEmpty();
        }
      } else if (left.getKind() == SqlKind.FLOOR) {
        final RexCall floorCall = (RexCall) left;
        if (floorCall.operands.get(0).getKind() == SqlKind.INPUT_REF) {
          final int index = getColumnIndex(floorCall.operands.get(0));
          if (floorCall.operands.size() > 1
              && floorCall.operands.get(1).getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
            // timezone related floor filters
            final TimeUnitRange floorToTimeRange = ((RexLiteral) floorCall.operands.get(1))
                .getValueAs(TimeUnitRange.class);
            final Long effectivePushDownTimestamp;
            switch (floorToTimeRange) {
            // TODO - get timezone from DateContext/Framework
            case MINUTE:
              effectivePushDownTimestamp = Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L)
                  .truncatedTo(ChronoUnit.MINUTES).toEpochMilli() * 1000L;
              break;
            case HOUR:
              effectivePushDownTimestamp = Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L)
                  .truncatedTo(ChronoUnit.HOURS).toEpochMilli() * 1000L;
              break;
            case DAY:
              effectivePushDownTimestamp = Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L)
                  .truncatedTo(ChronoUnit.DAYS).toEpochMilli() * 1000L;
              break;
            case MONTH: // Month & Year require ZonedDateTime as truncation greater than a DAY is not
                        // allowed on Instant
              effectivePushDownTimestamp = ZonedDateTime
                  .ofInstant(Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L), ZoneOffset.UTC)
                  .with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli()
                  * 1000L;
              break;
            case YEAR: // Month & Year require ZonedDateTime as truncation greater than a DAY is not
                       // allowed on Instant
              effectivePushDownTimestamp = ZonedDateTime
                  .ofInstant(Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L), ZoneOffset.UTC)
                  .with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli()
                  * 1000L;
              break;
            default:
              allExpressionsConverted = false;
              return setEmpty();
            }
            switch (parent.getOperator().getKind()) {
            case GREATER_THAN_OR_EQUAL: // Apply default behavior
            case LESS_THAN: // Apply default behavior
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, maybeOp.get(), effectivePushDownTimestamp)));
            case GREATER_THAN: // columnValue >= (effectivePushDownTimestamp + 1 time interval)
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER_EQUAL,
                      shiftTimestampByOneInterval(effectivePushDownTimestamp, floorToTimeRange, true))));
            case LESS_THAN_OR_EQUAL: // columnValue < (effectivePushDownTimestamp + 1 time interval)
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS,
                      shiftTimestampByOneInterval(effectivePushDownTimestamp, floorToTimeRange, true))));
            case PERIOD_EQUALS:
            case EQUALS:
              // columnValue >= effectivePushTimestamp && columnValue <
              // (effectivePushDownTimestamp + 1 time interval)
              return mergePredicateLists(SqlKind.AND,
                  Collections.singletonList(Collections.singletonList(new ComparisonPredicate(index,
                      KuduPredicate.ComparisonOp.GREATER_EQUAL, effectivePushDownTimestamp))),
                  Collections.singletonList(
                      Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS,
                          shiftTimestampByOneInterval(effectivePushDownTimestamp, floorToTimeRange, true)))));
            case NOT_EQUALS:
              // columnValue < effectivePushDownTimestamp || columnValue >=
              // (effectivePushDownValue + 1)
              return mergePredicateLists(SqlKind.OR,
                  Collections.singletonList(Collections.singletonList(
                      new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS, effectivePushDownTimestamp))),
                  Collections.singletonList(
                      Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER_EQUAL,
                          shiftTimestampByOneInterval(effectivePushDownTimestamp, floorToTimeRange, true)))));
            default:
              allExpressionsConverted = false;
              return setEmpty();
            }
          } else {
            final Object effectivePushDownFloorValue;
            switch (literal.getType().getSqlTypeName()) {
            case FLOAT:
              effectivePushDownFloorValue = (float) Math.floor((Float) castLiteral(literal));
              break;
            case DOUBLE:
              effectivePushDownFloorValue = Math.floor((Double) castLiteral(literal));
              break;
            case DECIMAL:
              effectivePushDownFloorValue = ((BigDecimal) castLiteral(literal)).setScale(0, BigDecimal.ROUND_FLOOR);
              break;
            default:
              allExpressionsConverted = false;
              return setEmpty();
            }
            switch (parent.getOperator().getKind()) {
            case GREATER_THAN_OR_EQUAL: // Apply default behavior
            case LESS_THAN: // Apply default behavior
              return Collections.singletonList(Collections
                  .singletonList(new ComparisonPredicate(index, maybeOp.get(), effectivePushDownFloorValue)));
            case GREATER_THAN: // columnValue >= (effectivePushDownValue + 1)
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER_EQUAL,
                      (literal.getType().getSqlTypeName() == SqlTypeName.FLOAT)
                          ? (Float) effectivePushDownFloorValue + new Float(1.0)
                          : (literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE)
                              ? (Double) effectivePushDownFloorValue + new Double(1.0)
                              : ((BigDecimal) effectivePushDownFloorValue).add(new BigDecimal("1")))));
            case LESS_THAN_OR_EQUAL: // columnValue < (effectivePushDownValue + 1)
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS,
                      (literal.getType().getSqlTypeName() == SqlTypeName.FLOAT)
                          ? (Float) effectivePushDownFloorValue + new Float(1.0)
                          : (literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE)
                              ? (Double) effectivePushDownFloorValue + new Double(1.0)
                              : ((BigDecimal) effectivePushDownFloorValue).add(new BigDecimal("1")))));
            case EQUALS:
              // columnValue >= effectivePushDownValue && columnValue <
              // (effectivePushDownValue + 1)
              return mergePredicateLists(SqlKind.AND,
                  Collections.singletonList(Collections.singletonList(new ComparisonPredicate(index,
                      KuduPredicate.ComparisonOp.GREATER_EQUAL, effectivePushDownFloorValue))),
                  Collections.singletonList(
                      Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS,
                          (literal.getType().getSqlTypeName() == SqlTypeName.FLOAT)
                              ? (Float) effectivePushDownFloorValue + new Float(1.0)
                              : (literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE)
                                  ? (Double) effectivePushDownFloorValue + new Double(1.0)
                                  : ((BigDecimal) effectivePushDownFloorValue).add(new BigDecimal("1"))))));
            case NOT_EQUALS:
              // columnValue < effectivePushDownValue || columnValue >=
              // (effectivePushDownValue + 1)
              return mergePredicateLists(SqlKind.OR,
                  Collections.singletonList(Collections.singletonList(
                      new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS, effectivePushDownFloorValue))),
                  Collections.singletonList(
                      Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER_EQUAL,
                          (literal.getType().getSqlTypeName() == SqlTypeName.FLOAT)
                              ? (Float) effectivePushDownFloorValue + new Float(1.0)
                              : (literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE)
                                  ? (Double) effectivePushDownFloorValue + new Double(1.0)
                                  : ((BigDecimal) effectivePushDownFloorValue).add(new BigDecimal("1"))))));
            default:
              allExpressionsConverted = false;
              return setEmpty();
            }
          }
        }
      } else if (left.getKind() == SqlKind.CEIL) {
        final RexCall floorCall = (RexCall) left;
        if (floorCall.operands.get(0).getKind() == SqlKind.INPUT_REF) {
          final int index = getColumnIndex(floorCall.operands.get(0));
          if (floorCall.operands.size() > 1
              && floorCall.operands.get(1).getType().getSqlTypeName() == SqlTypeName.SYMBOL) {
            // timezone related ceil filters
            final TimeUnitRange floorToTimeRange = ((RexLiteral) floorCall.operands.get(1))
                .getValueAs(TimeUnitRange.class);
            final Long effectivePushDownTimestamp;
            switch (floorToTimeRange) {
            // TODO - get timezone from DateContext/Framework
            case MINUTE:
              effectivePushDownTimestamp = Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L)
                  .plus(1L, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.MINUTES).toEpochMilli() * 1000L;
              break;
            case HOUR:
              effectivePushDownTimestamp = Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L)
                  .plus(1L, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS).toEpochMilli() * 1000L;
              break;
            case DAY:
              effectivePushDownTimestamp = Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L)
                  .plus(1L, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS).toEpochMilli() * 1000L;
              break;
            case MONTH: // Month & Year require ZonedDateTime as truncation greater than a DAY is not
                        // allowed on Instant
              effectivePushDownTimestamp = ZonedDateTime
                  .ofInstant(Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L), ZoneOffset.UTC)
                  .plus(1L, ChronoUnit.MONTHS).with(TemporalAdjusters.firstDayOfMonth()).truncatedTo(ChronoUnit.DAYS)
                  .toInstant().toEpochMilli() * 1000L;
              break;
            case YEAR: // Month & Year require ZonedDateTime as truncation greater than a DAY is not
                       // allowed on Instant
              effectivePushDownTimestamp = ZonedDateTime
                  .ofInstant(Instant.ofEpochMilli(((Long) castLiteral(literal)) / 1000L), ZoneOffset.UTC)
                  .plus(1L, ChronoUnit.YEARS).with(TemporalAdjusters.firstDayOfYear()).truncatedTo(ChronoUnit.DAYS)
                  .toInstant().toEpochMilli() * 1000L;
              break;
            default:
              allExpressionsConverted = false;
              return setEmpty();
            }
            switch (parent.getOperator().getKind()) {
            case GREATER_THAN: // Apply default behavior
            case LESS_THAN_OR_EQUAL: // Apply default behavior
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, maybeOp.get(), effectivePushDownTimestamp)));
            case GREATER_THAN_OR_EQUAL: // columnValue > (effectivePushDownTimestamp - 1 time interval)
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER,
                      shiftTimestampByOneInterval(effectivePushDownTimestamp, floorToTimeRange, false))));
            case LESS_THAN: // columnValue <= (effectivePushDownTimestamp - 1 time interval)
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS_EQUAL,
                      shiftTimestampByOneInterval(effectivePushDownTimestamp, floorToTimeRange, false))));
            case PERIOD_EQUALS:
            case EQUALS:
              // columnValue > (effectivePushDownTimestamp - 1 time interval) && columnValue
              // <= (effectivePushDownTimestamp)
              return mergePredicateLists(SqlKind.AND,
                  Collections.singletonList(
                      Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER,
                          shiftTimestampByOneInterval(effectivePushDownTimestamp, floorToTimeRange, false)))),
                  Collections.singletonList(Collections.singletonList(new ComparisonPredicate(index,
                      KuduPredicate.ComparisonOp.LESS_EQUAL, effectivePushDownTimestamp))));
            case NOT_EQUALS:
              // columnValue <= (effectivePushDownTimestamp - 1) || columnValue >
              // effectivePushDownTimestamp
              return mergePredicateLists(SqlKind.OR,
                  Collections.singletonList(
                      Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS_EQUAL,
                          shiftTimestampByOneInterval(effectivePushDownTimestamp, floorToTimeRange, false)))),
                  Collections.singletonList(Collections.singletonList(
                      new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER, effectivePushDownTimestamp))));
            default:
              allExpressionsConverted = false;
              return setEmpty();
            }
          } else {
            final Object effectivePushDownCeilValue;
            switch (literal.getType().getSqlTypeName()) {
            case FLOAT:
              effectivePushDownCeilValue = (float) Math.ceil((Float) castLiteral(literal));
              break;
            case DOUBLE:
              effectivePushDownCeilValue = Math.ceil((Double) castLiteral(literal));
              break;
            case DECIMAL:
              effectivePushDownCeilValue = ((BigDecimal) castLiteral(literal)).setScale(0, BigDecimal.ROUND_CEILING);
              break;
            default:
              allExpressionsConverted = false;
              return setEmpty();
            }
            switch (parent.getOperator().getKind()) {
            case GREATER_THAN: // Apply default behavior
            case LESS_THAN_OR_EQUAL: // Apply default behavior
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, maybeOp.get(), effectivePushDownCeilValue)));
            case GREATER_THAN_OR_EQUAL: // columnValue > (effectivePushDownValue - 1 time interval)
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER,
                      (literal.getType().getSqlTypeName() == SqlTypeName.FLOAT)
                          ? (Float) effectivePushDownCeilValue - new Float(1.0)
                          : (literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE)
                              ? (Double) effectivePushDownCeilValue - new Double(1.0)
                              : ((BigDecimal) effectivePushDownCeilValue).subtract(new BigDecimal("1")))));
            case LESS_THAN: // columnValue <= (effectivePushDownValue - 1 time interval)
              return Collections.singletonList(
                  Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS_EQUAL,
                      (literal.getType().getSqlTypeName() == SqlTypeName.FLOAT)
                          ? (Float) effectivePushDownCeilValue - new Float(1.0)
                          : (literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE)
                              ? (Double) effectivePushDownCeilValue - new Double(1.0)
                              : ((BigDecimal) effectivePushDownCeilValue).subtract(new BigDecimal("1")))));
            case EQUALS:
              // columnValue > (effectivePushDownValue - 1) && columnValue <=
              // (effectivePushDownValue)
              return mergePredicateLists(SqlKind.AND,
                  Collections.singletonList(
                      Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER,
                          (literal.getType().getSqlTypeName() == SqlTypeName.FLOAT)
                              ? (Float) effectivePushDownCeilValue - new Float(1.0)
                              : (literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE)
                                  ? (Double) effectivePushDownCeilValue - new Double(1.0)
                                  : ((BigDecimal) effectivePushDownCeilValue).subtract(new BigDecimal("1"))))),
                  Collections.singletonList(Collections.singletonList(new ComparisonPredicate(index,
                      KuduPredicate.ComparisonOp.LESS_EQUAL, effectivePushDownCeilValue))));
            case NOT_EQUALS:
              // columnValue <= (effectivePushDownValue - 1) || columnValue >
              // effectivePushDownValue
              return mergePredicateLists(SqlKind.OR,
                  Collections.singletonList(
                      Collections.singletonList(new ComparisonPredicate(index, KuduPredicate.ComparisonOp.LESS_EQUAL,
                          (literal.getType().getSqlTypeName() == SqlTypeName.FLOAT)
                              ? (Float) effectivePushDownCeilValue - new Float(1.0)
                              : (literal.getType().getSqlTypeName() == SqlTypeName.DOUBLE)
                                  ? (Double) effectivePushDownCeilValue - new Double(1.0)
                                  : ((BigDecimal) effectivePushDownCeilValue).subtract(new BigDecimal("1"))))),
                  Collections.singletonList(Collections.singletonList(
                      new ComparisonPredicate(index, KuduPredicate.ComparisonOp.GREATER, effectivePushDownCeilValue))));
            default:
              allExpressionsConverted = false;
              return setEmpty();
            }
          }
        }
      }
    }
    return setEmpty();
  }

  public List<List<CalciteKuduPredicate>> visitOver(RexOver over, RexCall parent) {
    return setEmpty();
  }

  public List<List<CalciteKuduPredicate>> visitCorrelVariable(RexCorrelVariable correlVariable, RexCall parent) {
    return setEmpty();
  }

  public List<List<CalciteKuduPredicate>> visitDynamicParam(RexDynamicParam dynamicParam, RexCall parent) {
    return setEmpty();
  }

  public List<List<CalciteKuduPredicate>> visitRangeRef(RexRangeRef rangeRef, RexCall parent) {
    return setEmpty();
  }

  public List<List<CalciteKuduPredicate>> visitFieldAccess(RexFieldAccess fieldAccess, RexCall parent) {
    return setEmpty();
  }

  public List<List<CalciteKuduPredicate>> visitSubQuery(RexSubQuery subQuery, RexCall parent) {
    return setEmpty();
  }

  public List<List<CalciteKuduPredicate>> visitTableInputRef(RexTableInputRef ref, RexCall parent) {
    return setEmpty();
  }

  public List<List<CalciteKuduPredicate>> visitPatternFieldRef(RexPatternFieldRef ref, RexCall parent) {
    return setEmpty();
  }

  private List<List<CalciteKuduPredicate>> setEmpty() {
    allExpressionsConverted = false;
    return Collections.emptyList();
  }
}
