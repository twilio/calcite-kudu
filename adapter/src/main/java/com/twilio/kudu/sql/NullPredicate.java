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

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduPredicate;

/**
 * An implementation of {@link CalciteKuduPredicate} that pushes down both NOT
 * NULL and IS NULL to Kudu.
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
    if (!not && !invertValue) {
      return KuduPredicate.newIsNullPredicate(columnSchema);
    } else if (!not && invertValue) {
      return KuduPredicate.newIsNotNullPredicate(columnSchema);
    } else if (not && !invertValue) {
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

  @Override
  public String toString() {
    return "NullPredicate [columnIdx=" + columnIdx + ", not=" + not + "]";
  }
}
