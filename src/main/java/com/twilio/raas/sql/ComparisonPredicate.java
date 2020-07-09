package com.twilio.raas.sql;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;

public final class ComparisonPredicate extends CalciteKuduPredicate {
  public final KuduPredicate.ComparisonOp operation;
  public final Object rightHandValue;
  public final int columnIdx;

  public ComparisonPredicate(final int columnIdx, final KuduPredicate.ComparisonOp operation, final Object rightHandValue) {
    this.columnIdx = columnIdx;
    this.operation = operation;
    this.rightHandValue = rightHandValue;
  }

  @Override
  public int getColumnIdx() {
    return columnIdx;
  }

  @Override
  public String explainPredicate(final ColumnSchema schema) {
    return String.format("%s %s %s", schema.getName(), operation.name(), rightHandValue);
  }

  @Override
  public KuduPredicate toPredicate(ColumnSchema columnSchema, boolean invertValue) {
    if (rightHandValue instanceof Boolean) {
      return KuduPredicate
        .newComparisonPredicate(columnSchema, operation,(Boolean) rightHandValue);
    }
    else if (rightHandValue instanceof BigDecimal) {
      return KuduPredicate
        .newComparisonPredicate(columnSchema, operation,(BigDecimal) rightHandValue);
    }
    else if (rightHandValue instanceof Double) {
      return KuduPredicate
        .newComparisonPredicate(columnSchema, operation,(Double) rightHandValue);
    }
    else if (rightHandValue instanceof Float) {
      return KuduPredicate
        .newComparisonPredicate(columnSchema, operation,(Float) rightHandValue);
    }
    else if (rightHandValue instanceof Timestamp) {
      return invertValue ?
        KuduPredicate
        .newComparisonPredicate(columnSchema, invertComparisonOp(operation),
            new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS - ((Timestamp)rightHandValue).toInstant().toEpochMilli())) :
        KuduPredicate
        .newComparisonPredicate(columnSchema, operation,(Timestamp) rightHandValue);
    }
    else if (rightHandValue instanceof String) {
      return KuduPredicate
        .newComparisonPredicate(columnSchema, operation,(String) rightHandValue);
    }
    else if (rightHandValue instanceof Byte) {
      return invertValue ?
        KuduPredicate
        .newComparisonPredicate(columnSchema, invertComparisonOp(operation), 1L * Byte.MAX_VALUE - (Byte)rightHandValue) :
        KuduPredicate
        .newComparisonPredicate(columnSchema, operation,(Byte) rightHandValue);
    }
    else if (rightHandValue instanceof Short) {
      return invertValue ?
        KuduPredicate
        .newComparisonPredicate(columnSchema, invertComparisonOp(operation), 1L * Short.MAX_VALUE - (Short)rightHandValue) :
        KuduPredicate
        .newComparisonPredicate(columnSchema, operation,(Short) rightHandValue);
    }
    else if (rightHandValue instanceof Integer) {
      switch(columnSchema.getType()) {
      case INT8: return invertValue ?
          KuduPredicate
          .newComparisonPredicate(columnSchema, invertComparisonOp(operation), 1L * Byte.MAX_VALUE - new Byte(rightHandValue.toString())) :
        KuduPredicate
          .newComparisonPredicate(columnSchema, operation,new Byte(rightHandValue.toString()));
      case INT16: return invertValue ?
          KuduPredicate
          .newComparisonPredicate(columnSchema, invertComparisonOp(operation), 1L * Short.MAX_VALUE - new Short(rightHandValue.toString())) :
        KuduPredicate
          .newComparisonPredicate(columnSchema, operation,new Short(rightHandValue.toString()));
      case INT32: return invertValue ?
          KuduPredicate
          .newComparisonPredicate(columnSchema, invertComparisonOp(operation), 1L * Integer.MAX_VALUE - (Integer)rightHandValue) :
        KuduPredicate
          .newComparisonPredicate(columnSchema, operation,(Integer) rightHandValue);
      case INT64: return invertValue ?
          // UNIXTIME_MICROS wont come in as an Integer. So safe to ignore
          KuduPredicate
          .newComparisonPredicate(columnSchema, invertComparisonOp(operation), Long.MAX_VALUE - (Long)rightHandValue) :
        KuduPredicate
          .newComparisonPredicate(columnSchema, operation,(Long) rightHandValue);
      case DECIMAL: return KuduPredicate
          .newComparisonPredicate(columnSchema, operation,BigDecimal.valueOf(((Number) rightHandValue).longValue()));
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
          .newComparisonPredicate(columnSchema, invertComparisonOp(operation),
              CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - (Long)rightHandValue) :
          KuduPredicate
          .newComparisonPredicate(columnSchema, invertComparisonOp(operation),
              Long.MAX_VALUE - (Long)rightHandValue);
      }
      else if (columnSchema.getType() == Type.DECIMAL) {
        return KuduPredicate
          .newComparisonPredicate(columnSchema, operation,BigDecimal.valueOf(((Number) rightHandValue).longValue()));
      }
      return KuduPredicate.newComparisonPredicate(columnSchema, operation,(Long) rightHandValue);
    }

    // @TODO: this covers all the possible types known in kudu 1.9
    // So.... this shouldn't ever happen
    return null;
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
    ComparisonPredicate other = (ComparisonPredicate) obj;
    if (columnIdx != other.columnIdx)
      return false;
    if (operation != other.operation)
      return false;
    if (rightHandValue == null) {
      if (other.rightHandValue != null)
        return false;
    } else if (!rightHandValue.equals(other.rightHandValue))
      return false;
    return true;
  }
}
