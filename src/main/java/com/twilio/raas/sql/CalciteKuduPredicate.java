package com.twilio.raas.sql;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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

    private final boolean inList;

    public CalciteKuduPredicate(final int columnIdx, final KuduPredicate.ComparisonOp operation, final Object rightHandValue) {
        this.columnIdx = columnIdx;
        this.operation = Optional.ofNullable(operation);
        this.rightHandValue = rightHandValue;
        inList = false;
    }

    public CalciteKuduPredicate(final int columnIdx, List<Object> rightHandValues) {
        this.columnIdx = columnIdx;
        this.operation = Optional.empty();
        this.inList = true;
        this.rightHandValue = rightHandValues;
    }

    /**
     * Transforms this POJO into a proper {@link KuduPredicate}
     *
     * @param columnSchema  column schema to use for the predicate
     * @param invertValue   true if the column is stored in descending order
     *
     * @return {@code KuduPredicate} that represents this POJO
     */
    public KuduPredicate toPredicate(ColumnSchema columnSchema, boolean invertValue) {
        if (inList) {
            switch(columnSchema.getType()) {
            case STRING:
                final List<String> stringValues = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> o.toString())
                    .collect(Collectors.toList());
                return KuduPredicate
                    .newInListPredicate(columnSchema, stringValues);
            case BOOL:
                final List<Boolean> booleanValues = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> (Boolean) o)
                    .collect(Collectors.toList());
                return KuduPredicate
                        .newInListPredicate(columnSchema, booleanValues);
            case INT8:
                final List<Byte> byteValues = ((List<Object>) this.rightHandValue).stream().map(o -> (Byte) o)
                        .collect(Collectors.toList());
                return KuduPredicate.newInListPredicate(columnSchema, byteValues);

            case INT16:
                final List<Short> shortValues = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> (Short) o)
                    .collect(Collectors.toList());
                return KuduPredicate.newInListPredicate(columnSchema, shortValues);

            case INT32:
                final List<Integer> intValues = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> (Integer) o)
                    .collect(Collectors.toList());
                return KuduPredicate.newInListPredicate(columnSchema, intValues);

            case INT64:
            case UNIXTIME_MICROS:
                final List<Long> longValues = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> (Long) o)
                    .collect(Collectors.toList());
                return KuduPredicate.newInListPredicate(columnSchema, longValues);

            case FLOAT:
                final List<Float> floatValues = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> (Float) o)
                    .collect(Collectors.toList());

                return KuduPredicate
                        .newInListPredicate(columnSchema, floatValues);

            case DOUBLE:
                final List<Double> doubleValues  = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> (Double) o)
                    .collect(Collectors.toList());
                return KuduPredicate
                        .newInListPredicate(columnSchema, doubleValues);

            case DECIMAL:
                final List<BigDecimal> decimalValues = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> (BigDecimal) o)
                    .collect(Collectors.toList());
                return KuduPredicate.newInListPredicate(columnSchema, decimalValues);

            case BINARY:
                // @TODO: this is weird.
                final List<byte[]> binaryValues = ((List<Object>) this.rightHandValue)
                    .stream()
                    .map(o -> (byte[]) o)
                    .collect(Collectors.toList());
                return KuduPredicate
                        .newInListPredicate(columnSchema, binaryValues);
            default:
                throw new IllegalArgumentException(
                        String.format("Cannot use in list with type %s", columnSchema.getType()));
            }
        }
        return this.operation
            .map(op -> {
                    if (rightHandValue instanceof Boolean) {
                        return KuduPredicate
                            .newComparisonPredicate(columnSchema, op, (Boolean) rightHandValue);
                    }
                    else if (rightHandValue instanceof BigDecimal) {
                        return KuduPredicate
                            .newComparisonPredicate(columnSchema, op, (BigDecimal) rightHandValue);
                    }
                    else if (rightHandValue instanceof Double) {
                        return KuduPredicate
                            .newComparisonPredicate(columnSchema, op, (Double) rightHandValue);
                    }
                    else if (rightHandValue instanceof Float) {
                        return KuduPredicate
                            .newComparisonPredicate(columnSchema, op, (Float) rightHandValue);
                    }
                    else if (rightHandValue instanceof Timestamp) {
                      return invertValue ?
                          KuduPredicate
                              .newComparisonPredicate(columnSchema, invertComparisonOp(op),
                                  new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS - ((Timestamp)rightHandValue).toInstant().toEpochMilli())) :
                          KuduPredicate
                              .newComparisonPredicate(columnSchema, op, (Timestamp) rightHandValue);
                    }
                    else if (rightHandValue instanceof String) {
                        return KuduPredicate
                            .newComparisonPredicate(columnSchema, op, (String) rightHandValue);
                    }
                    else if (rightHandValue instanceof Byte) {
                      return invertValue ?
                          KuduPredicate
                              .newComparisonPredicate(columnSchema, invertComparisonOp(op), 1L * Byte.MAX_VALUE - (Byte)rightHandValue) :
                          KuduPredicate
                              .newComparisonPredicate(columnSchema, op, (Byte) rightHandValue);
                    }
                    else if (rightHandValue instanceof Short) {
                      return invertValue ?
                          KuduPredicate
                              .newComparisonPredicate(columnSchema, invertComparisonOp(op), 1L * Short.MAX_VALUE - (Short)rightHandValue) :
                          KuduPredicate
                              .newComparisonPredicate(columnSchema, op, (Short) rightHandValue);
                    }
                    else if (rightHandValue instanceof Integer) {
                      switch(columnSchema.getType()) {
                        case INT8: return invertValue ?
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, invertComparisonOp(op), 1L * Byte.MAX_VALUE - new Byte(rightHandValue.toString())) :
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, op, new Byte(rightHandValue.toString()));
                        case INT16: return invertValue ?
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, invertComparisonOp(op), 1L * Short.MAX_VALUE - new Short(rightHandValue.toString())) :
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, op, new Short(rightHandValue.toString()));
                        case INT32: return invertValue ?
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, invertComparisonOp(op), 1L * Integer.MAX_VALUE - (Integer)rightHandValue) :
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, op, (Integer) rightHandValue);
                        case INT64: return invertValue ?
                            // UNIXTIME_MICROS wont come in as an Integer. So safe to ignore
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, invertComparisonOp(op), Long.MAX_VALUE - (Long)rightHandValue) :
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, op, (Long) rightHandValue);
                        case DECIMAL: return KuduPredicate
                                .newComparisonPredicate(columnSchema, op, BigDecimal.valueOf(((Number) rightHandValue).longValue()));
                        default: throw new IllegalArgumentException(
                            String.format("Passed in predicate value:%s is incompatible with table datatype:%s",
                                          rightHandValue.toString(),
                                          columnSchema.getName()));
                      }
                    }
                    else if (rightHandValue instanceof Long) {
                      if (invertValue) {
                        // Long as well as UNIXTIME_MICROS return Long value
                        return (columnSchema.getType() == Type.UNIXTIME_MICROS) ?
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, invertComparisonOp(op),
                                    CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - (Long)rightHandValue) :
                            KuduPredicate
                                .newComparisonPredicate(columnSchema, invertComparisonOp(op),
                                    Long.MAX_VALUE - (Long)rightHandValue);
                      }
                      else if (columnSchema.getType() == Type.DECIMAL) {
                        return KuduPredicate
                          .newComparisonPredicate(columnSchema, op, BigDecimal.valueOf(((Number) rightHandValue).longValue()));
                      }
                      return KuduPredicate.newComparisonPredicate(columnSchema, op, (Long) rightHandValue);
                    }
                    // @TODO: this covers all the possible types known in kudu 1.9
                    // So.... this shouldn't ever happen
                    return null;
                })
            .orElse(KuduPredicate.newIsNullPredicate(columnSchema));
    }

  /**
   * Transforms this POJO into a proper {@link KuduPredicate}
   *
   * @param calciteKuduTable table to use to generate predicate
   * @return {@code KuduPredicate} that represents this POJO
   */
  public KuduPredicate toPredicate(CalciteKuduTable calciteKuduTable) {
    final Schema tableSchema = calciteKuduTable.getKuduTable().getSchema();
    final ColumnSchema columnsSchema = tableSchema.getColumnByIndex(columnIdx);
    final boolean invertValue = calciteKuduTable.isColumnOrderedDesc(columnIdx);
    return toPredicate(columnsSchema, invertValue);
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
        result = prime * result + operation.hashCode();
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
        if (!operation.equals(other.operation))
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
