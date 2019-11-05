package com.twilio.raas.sql;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduPredicate;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import org.apache.kudu.Schema;

/**
 * A simple "case" class / POJO to help with code generation in
 * {@link com.twilio.raas.sql.rules.KuduToEnumerableConverter}.
 * Simplifies the {@link org.apache.calcite.linq4j.tree.Expression}
 * generation so it is more readable
 */
public final class CalciteKuduPredicate {
    public final String columnName;
    /**
     * When present, use it for a comparison Predicate,
     * _otherwise_ it is a is Null Predicate
     *
     * @TODO: there is a way to create a comparison predicate in
     * such a way that is equivalent to isNullPredicate and 
     * isNotNullPredicate.
     */
    public final Optional<KuduPredicate.ComparisonOp> operation;
    public final Object rightHandValue;

    public CalciteKuduPredicate(final String columnName, final KuduPredicate.ComparisonOp operation, final Object rightHandValue) {
        this.columnName = columnName;
        this.operation = Optional.ofNullable(operation);
        this.rightHandValue = rightHandValue;
    }

    /**
     * Transforms this POJO into a proper {@link KuduPredicate}
     *
     * @param tableSchema  table schema to use for the predicate
     *
     * @return {@code KuduPredicate} that represents this POJO
     */
    public KuduPredicate toPredicate(Schema tableSchema, List<Integer> descendingSortedDateTimeFieldIndices) {
        final ColumnSchema columnsSchema = tableSchema.getColumn(columnName);
        return this.operation
            .map(op -> {
                    if (rightHandValue instanceof Boolean) {
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (Boolean) rightHandValue);
                    }
                    else if (rightHandValue instanceof BigDecimal) {
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (BigDecimal) rightHandValue);
                    }
                    else if (rightHandValue instanceof Double) {
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (Double) rightHandValue);
                    }
                    else if (rightHandValue instanceof Float) {
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (Float) rightHandValue);
                    }
                    else if (rightHandValue instanceof Timestamp) {
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (Timestamp) rightHandValue);
                    }
                    else if (rightHandValue instanceof String) {
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (String) rightHandValue);
                    }
                    else if (rightHandValue instanceof Integer) {
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (Integer) rightHandValue);
                    }
                    else if (rightHandValue instanceof Long) {
                      if(descendingSortedDateTimeFieldIndices.contains(tableSchema.getColumnIndex(columnName))) {
                        // subtract epoch microseconds from Long.MAX_VALUE
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, invertComparisonOp(op),
                                JDBCQueryRunner.EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - (Long)rightHandValue);
                      }
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (Long) rightHandValue);
                    }
                    // @TODO: this covers all the possible types known in kudu 1.9
                    // So.... this shouldn't ever happen
                    return null;
                })
            .orElse(KuduPredicate.newIsNullPredicate(columnsSchema));
    }

    private KuduPredicate.ComparisonOp invertComparisonOp(final KuduPredicate.ComparisonOp currentOp) {
      switch(currentOp) {
        case GREATER: return KuduPredicate.ComparisonOp.LESS;
        case GREATER_EQUAL: return KuduPredicate.ComparisonOp.LESS_EQUAL;
        case LESS: return KuduPredicate.ComparisonOp.GREATER;
        case LESS_EQUAL: return KuduPredicate.ComparisonOp.GREATER_EQUAL;
        case EQUAL: return currentOp;
        default: throw new IllegalArgumentException(
            String.format("Passed in an Operator that doesn't make sense for Kudu Predicates: %s", currentOp));
      }
    }
}
