package com.twilio.raas.sql;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduPredicate;

/**
 * An implementation of {@link CalciteKuduPredicate} that pushes down both NOT NULL and IS NULL to
 * Kudu.
 *
 * @see {@link KuduPredicate#newIsNullPredicate(ColumnSchema)}
 * @see {@link KuduPredicate#newIsNotNullPredicate(ColumnSchema)}
 */
public final class NullPredicate extends CalciteKuduPredicate {
  public final int columnIdx;
  private boolean not;

  public NullPredicate(final int columnIdx, final boolean notNull) {
    this.not = notNull;
    this.columnIdx = columnIdx;
  }

  @Override
  public int getColumnIdx() {
    return columnIdx;
  }

  @Override
  public String explainPredicate(final ColumnSchema schema) {
    if (not) {
      return String.format("%s IS NOT NULL", schema.getName());
    }
    return String.format("%s IS NULL", schema.getName());
  }

  @Override
  public KuduPredicate toPredicate(ColumnSchema columnSchema, boolean invertValue) {
    if (! not && ! invertValue) {
      return KuduPredicate.newIsNullPredicate(columnSchema);
    }
    else if (! not && invertValue) {
      return KuduPredicate.newIsNotNullPredicate(columnSchema);
    }
    else if (not && ! invertValue) {
      return KuduPredicate.newIsNotNullPredicate(columnSchema);
    }
    // if (not && invertValue)
    else {
      return KuduPredicate.newIsNullPredicate(columnSchema);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + columnIdx;
    result = prime * result + (not ? 1231 : 1237);
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
    NullPredicate other = (NullPredicate) obj;
    if (columnIdx != other.columnIdx)
      return false;
    if (not != other.not)
      return false;
    return true;
  }
}
