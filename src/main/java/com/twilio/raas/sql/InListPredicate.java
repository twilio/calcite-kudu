package com.twilio.raas.sql;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduPredicate;

/**
 * An implementation of {@link CalciteKuduPredicate} that creates an IN LIST predicate to {@link KuduEnumerable}
 *
 * @see {@link KuduPredicate#newInListPredicate(ColumnSchema, List)}
 */
public final class InListPredicate extends CalciteKuduPredicate {
  public final int columnIdx;

  private final List<Object> values;

  public InListPredicate(final int columnIdx, final List<Object> values) {
    this.columnIdx = columnIdx;
    this.values = values;
  }

  @Override
  public int getColumnIdx() {
    return columnIdx;
  }

  @Override
  public String explainPredicate(final ColumnSchema schema) {
    return String.format("%s IN %s", schema.getName(), values);
  }

  @Override
  public KuduPredicate toPredicate(final ColumnSchema columnSchema, final boolean invertValue) {
      switch(columnSchema.getType()) {
      case STRING:
          final List<String> stringValues = this.values
              .stream()
              .map(o -> o.toString())
              .collect(Collectors.toList());
          return KuduPredicate
              .newInListPredicate(columnSchema, stringValues);
      case BOOL:
          final List<Boolean> booleanValues = this.values
              .stream()
              .map(o -> (Boolean) o)
              .collect(Collectors.toList());
          return KuduPredicate
              .newInListPredicate(columnSchema, booleanValues);
      case INT8:
          final List<Number> byteValues = this.values
            .stream()
            .map(o -> {
                  if (invertValue) {
                    return 1L * Byte.MAX_VALUE - (Byte) o;
                  }
                  return (Byte) o;
                })
            .collect(Collectors.toList());
          return KuduPredicate.newInListPredicate(columnSchema, byteValues);

      case INT16:
          final List<Number> shortValues = this.values
              .stream()
            .map(o -> {
                  if (invertValue) {
                    return 1L * Short.MAX_VALUE - ((Number) o).shortValue();
                  }
                  return ((Number) o).shortValue();
                })
              .collect(Collectors.toList());
          return KuduPredicate.newInListPredicate(columnSchema, shortValues);

      case INT32:
          final List<Number> intValues = this.values
              .stream()
            .map(o -> {
                  if (invertValue) {
                    return 1L * Integer.MAX_VALUE - ((Number) o).intValue();
                  }
                  return ((Number) o).intValue();
                })
            .collect(Collectors.toList());
          return KuduPredicate.newInListPredicate(columnSchema, intValues);

      case UNIXTIME_MICROS:
          final List<Long> timestampsInMicros;

          if (this.values.get(0) instanceof Timestamp) {
              timestampsInMicros = this.values
                  .stream()
                  .map(original -> {
                          final long ts = ((Timestamp) original)
                              .toInstant()
                              .toEpochMilli();

                          if (invertValue) {
                              return CalciteKuduTable.EPOCH_DAY_FOR_REVERSE_SORT.toEpochMilli() - ts;
                          }
                          else {
                              return ts;
                          }
                      })
                  .collect(Collectors.toList());
          }
          else if (this.values.get(0) instanceof Long) {
              timestampsInMicros = this.values
                  .stream()
                  .map(original -> {
                          final Long ts = ((Number) original).longValue();

                          if (invertValue) {
                              return CalciteKuduTable.EPOCH_DAY_FOR_REVERSE_SORT.toEpochMilli() - ts;
                          }
                          else {
                              return ts;
                          }
                      })
                  .collect(Collectors.toList());
          }
          else {
            throw new IllegalArgumentException(String.format("Cannot convert a %s into a UNIXTIME_MICROS for Kudu",
                this.values.get(0).getClass()));
          }

          return KuduPredicate.newInListPredicate(columnSchema, timestampsInMicros);

      case INT64:
          final List<Long> longValues = this.values
              .stream()
              .map(o -> {
                    final Long value = ((Number) o).longValue();
                    if (invertValue) {
                      return Long.MAX_VALUE - value;
                    }
                    else {
                      return value;
                    }
                  })
              .collect(Collectors.toList());
          return KuduPredicate.newInListPredicate(columnSchema, longValues);

      case FLOAT:
          final List<Float> floatValues = this.values
              .stream()
              .map(o -> (Float) o)
              .collect(Collectors.toList());

          return KuduPredicate
              .newInListPredicate(columnSchema, floatValues);

      case DOUBLE:
          final List<Double> doubleValues  = this.values
              .stream()
              .map(o -> (Double) o)
              .collect(Collectors.toList());
          return KuduPredicate
              .newInListPredicate(columnSchema, doubleValues);

      case DECIMAL:
          final List<BigDecimal> decimalValues = this.values
              .stream()
              .map(o -> (BigDecimal) o)
              .collect(Collectors.toList());
          return KuduPredicate.newInListPredicate(columnSchema, decimalValues);

      case BINARY:
          // @TODO: this is weird.
          final List<byte[]> binaryValues = this.values
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + columnIdx;
        result = prime * result + ((values == null) ? 0 : values.hashCode());
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
        InListPredicate other = (InListPredicate) obj;
        if (columnIdx != other.columnIdx)
            return false;
        if (values == null) {
            if (other.values != null)
                return false;
        } else if (!values.equals(other.values))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "InListPredicate [columnIdx=" + columnIdx + ", values=" + values + "]";
    }
}
