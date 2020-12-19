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
package com.twilio.kudu.sql;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;

/**
 * An implementation of {@link CalciteKuduPredicate} that creates comparison
 * {@link KuduPredicate}
 *
 * {@link KuduPredicate#newComparisonPredicate(ColumnSchema, org.apache.kudu.client.KuduPredicate.ComparisonOp, Object)}
 */
public final class ComparisonPredicate extends CalciteKuduPredicate {
  public final KuduPredicate.ComparisonOp operation;
  public final Object rightHandValue;
  public final int columnIdx;

  public ComparisonPredicate(final int columnIdx, final KuduPredicate.ComparisonOp operation,
      final Object rightHandValue) {
    this.columnIdx = columnIdx;
    this.operation = operation;
    this.rightHandValue = rightHandValue;
  }

  @Override
  public int getColumnIdx() {
    return columnIdx;
  }

  @Override
  public boolean inListOptimizationAllowed(final int columnIdx) {
    return columnIdx == this.columnIdx && operation == KuduPredicate.ComparisonOp.EQUAL;
  }

  @Override
  public String explainPredicate(final ColumnSchema schema) {
    return String.format("%s %s %s", schema.getName(), operation.name(), rightHandValue);
  }

  /**
   * In order to support descending ordered primary key column we invert the
   * column value (see MutationState.getColumnValue())
   */
  @Override
  public KuduPredicate toPredicate(ColumnSchema columnSchema, boolean invertValue) {
    if (rightHandValue instanceof Timestamp) {
      return invertValue
          ? KuduPredicate.newComparisonPredicate(columnSchema, invertComparisonOp(operation),
              new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS
                  - ((Timestamp) rightHandValue).toInstant().toEpochMilli()))
          : KuduPredicate.newComparisonPredicate(columnSchema, operation, (Timestamp) rightHandValue);
    } else if (rightHandValue instanceof Byte) {
      return invertValue
          ? KuduPredicate.newComparisonPredicate(columnSchema, invertComparisonOp(operation),
              (byte) (-1 - (Byte) rightHandValue))
          : KuduPredicate.newComparisonPredicate(columnSchema, operation, rightHandValue);
    } else if (rightHandValue instanceof Short) {
      return invertValue
          ? KuduPredicate.newComparisonPredicate(columnSchema, invertComparisonOp(operation),
              (short) (-1 - (Short) rightHandValue))
          : KuduPredicate.newComparisonPredicate(columnSchema, operation, rightHandValue);
    } else if (rightHandValue instanceof Integer) {
      if (columnSchema.getType() == Type.DECIMAL) {
        return KuduPredicate.newComparisonPredicate(columnSchema, operation,
            BigDecimal.valueOf(((Number) rightHandValue).longValue()));
      } else {
        return invertValue
            ? KuduPredicate.newComparisonPredicate(columnSchema, invertComparisonOp(operation),
                -1 - (Integer) rightHandValue)
            : KuduPredicate.newComparisonPredicate(columnSchema, operation, rightHandValue);
      }
    } else if (rightHandValue instanceof Long) {
      if (columnSchema.getType() == Type.DECIMAL) {
        return KuduPredicate.newComparisonPredicate(columnSchema, operation,
            BigDecimal.valueOf(((Number) rightHandValue).longValue()));
      } else if (invertValue) {
        // Long as well as UNIXTIME_MICROS return Long value
        return (columnSchema.getType() == Type.UNIXTIME_MICROS)
            ? KuduPredicate.newComparisonPredicate(columnSchema, invertComparisonOp(operation),
                CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - (long) rightHandValue)
            : KuduPredicate.newComparisonPredicate(columnSchema, invertComparisonOp(operation),
                -1l - (Long) rightHandValue);
      } else {
        return KuduPredicate.newComparisonPredicate(columnSchema, operation, rightHandValue);
      }
    } else {
      return KuduPredicate.newComparisonPredicate(columnSchema, operation, rightHandValue);
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

  @Override
  public String toString() {
    return "ComparisonPredicate [columnIdx=" + columnIdx + ", operation=" + operation + ", rightHandValue="
        + rightHandValue + "]";
  }
}
