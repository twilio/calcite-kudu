package com.twilio.raas.sql;

import java.sql.JDBCType;
import java.sql.Time;
import java.util.List;
import java.util.Optional;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.kudu.Schema;

/**
 * A simple "case" class / POJO to help with code generation in
 * {@link com.twilio.raas.sql.rules.KuduToEnumerableConverter}.
 * Simplifies the {@link org.apache.calcite.linq4j.tree.Expression}
 * generation so it is more readable
 */
public final class CalciteKuduPredicate {
    public final int columnIdx;
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

    public CalciteKuduPredicate(final int columnIdx, final KuduPredicate.ComparisonOp operation, final Object rightHandValue) {
        this.columnIdx = columnIdx;
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
    public KuduPredicate toPredicate(Schema tableSchema, List<Integer> descendingSortedFieldIndices) {
        final ColumnSchema columnsSchema = tableSchema.getColumnByIndex(columnIdx);
        final boolean invertValue = descendingSortedFieldIndices.contains(
            columnIdx);
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
                      return invertValue ?
                          KuduPredicate
                              .newComparisonPredicate(columnsSchema, invertComparisonOp(op),
                                  new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS - ((Timestamp)rightHandValue).toInstant().toEpochMilli())) :
                          KuduPredicate
                              .newComparisonPredicate(columnsSchema, op, (Timestamp) rightHandValue);
                    }
                    else if (rightHandValue instanceof String) {
                        return KuduPredicate
                            .newComparisonPredicate(columnsSchema, op, (String) rightHandValue);
                    }
                    else if (rightHandValue instanceof Byte) {
                      return invertValue ?
                          KuduPredicate
                              .newComparisonPredicate(columnsSchema, invertComparisonOp(op), Byte.MAX_VALUE - (Byte)rightHandValue) :
                          KuduPredicate
                              .newComparisonPredicate(columnsSchema, op, (Byte) rightHandValue);
                    }
                    else if (rightHandValue instanceof Short) {
                      return invertValue ?
                          KuduPredicate
                              .newComparisonPredicate(columnsSchema, invertComparisonOp(op), Short.MAX_VALUE - (Short)rightHandValue) :
                          KuduPredicate
                              .newComparisonPredicate(columnsSchema, op, (Short) rightHandValue);
                    }
                    else if (rightHandValue instanceof Integer) {
                      switch(tableSchema.getColumnByIndex(columnIdx).getType()) {
                        case INT8: return invertValue ?
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, invertComparisonOp(op), Byte.MAX_VALUE - new Byte(rightHandValue.toString())) :
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, op, new Byte(rightHandValue.toString()));
                        case INT16: return invertValue ?
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, invertComparisonOp(op), Short.MAX_VALUE - new Short(rightHandValue.toString())) :
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, op, new Short(rightHandValue.toString()));
                        case INT32: return invertValue ?
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, invertComparisonOp(op), Integer.MAX_VALUE - (Integer)rightHandValue) :
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, op, (Integer) rightHandValue);
                        case INT64: return invertValue ?
                            // UNIXTIME_MICROS wont come in as an Integer. So safe to ignore
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, invertComparisonOp(op), Long.MAX_VALUE - (Long)rightHandValue) :
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, op, (Long) rightHandValue);
                        case DECIMAL: return KuduPredicate
                                .newComparisonPredicate(columnsSchema, op, BigDecimal.valueOf(((Number) rightHandValue).longValue()));
                        default: throw new IllegalArgumentException(
                            String.format("Passed in predicate value:%s is incompatible with table datatype:%s",
                                          rightHandValue.toString(),
                                          tableSchema.getColumnByIndex(columnIdx).getType().getName()));
                      }
                    }
                    else if (rightHandValue instanceof Long) {
                      if (invertValue) {
                        // Long as well as UNIXTIME_MICROS return Long value
                        return (tableSchema.getColumnByIndex(columnIdx).getType() == Type.UNIXTIME_MICROS) ?
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, invertComparisonOp(op),
                                    CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - (Long)rightHandValue) :
                            KuduPredicate
                                .newComparisonPredicate(columnsSchema, invertComparisonOp(op),
                                    Long.MAX_VALUE - (Long)rightHandValue);
                      }
                      else if (tableSchema.getColumnByIndex(columnIdx).getType() == Type.DECIMAL) {
                        return KuduPredicate
                          .newComparisonPredicate(columnsSchema, op, BigDecimal.valueOf(((Number) rightHandValue).longValue()));
                      }
                      return KuduPredicate.newComparisonPredicate(columnsSchema, op, (Long) rightHandValue);
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + columnIdx;
        result = prime * result + ((operation == null) ? 0 : operation.hashCode());
        result = prime * result + ((rightHandValue == null) ? 0 : rightHandValue.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        CalciteKuduPredicate other = (CalciteKuduPredicate) obj;
        if (columnIdx != other.columnIdx)
            return false;
        if (operation == null) {
            if (other.operation != null)
                return false;
        } else if (!operation.equals(other.operation))
            return false;
        if (rightHandValue == null) {
            if (other.rightHandValue != null)
                return false;
        } else if (!rightHandValue.equals(other.rightHandValue))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "CalciteKuduPredicate [columnIdx=" + columnIdx + ", operation=" + operation + ", rightHandValue="
                + rightHandValue + "]";
    }
}
