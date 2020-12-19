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

import org.apache.kudu.Schema;

/**
 * A simple "case" class / POJO to help with code generation in
 * {@link com.twilio.kudu.sql.rules.KuduToEnumerableConverter}. Simplifies the
 * {@link org.apache.calcite.linq4j.tree.Expression} generation so it is more
 * readable
 */
public abstract class CalciteKuduPredicate {
  /**
   * Transforms this POJO into a proper {@link KuduPredicate}
   *
   * @param columnSchema column schema to use for the predicate
   * @param invertValue  true if the column is stored in descending order
   *
   * @return {@code KuduPredicate} that represents this POJO
   */
  public abstract KuduPredicate toPredicate(ColumnSchema columnSchema, boolean invertValue);

  /**
   * returns the column index for this predicate
   *
   * @return integer of the column in Kudu
   */
  public abstract int getColumnIdx();

  /**
   * Constructs a string used when generating the explain plan
   *
   * @param columnSchema Schema of the column from Kudu
   *
   * @return Formatted string from {@link org.apache.calcite.rel.RelWriter} to use
   */
  public abstract String explainPredicate(final ColumnSchema columnSchema);

  /**
   * Returns true or false if this can be considered for in list optimization.
   *
   * @param columnIdx the column index in the
   *                  {@link org.apache.kudu.client.KuduTable}
   *
   * @return true if this column and predicate can be optimized into an
   *         {@link InListPredicate}
   */
  public boolean inListOptimizationAllowed(final int columnIdx) {
    return false;
  }

  /**
   * Transforms this POJO into a proper {@link KuduPredicate}
   *
   * @param calciteKuduTable table to use to generate predicate
   * @return {@code KuduPredicate} that represents this POJO
   */
  public KuduPredicate toPredicate(CalciteKuduTable calciteKuduTable) {
    final Schema tableSchema = calciteKuduTable.getKuduTable().getSchema();
    final ColumnSchema columnsSchema = tableSchema.getColumnByIndex(getColumnIdx());
    final boolean invertValue = calciteKuduTable.isColumnOrderedDesc(getColumnIdx());
    return toPredicate(columnsSchema, invertValue);
  }

  protected KuduPredicate.ComparisonOp invertComparisonOp(final KuduPredicate.ComparisonOp currentOp) {
    switch (currentOp) {
    case GREATER:
      return KuduPredicate.ComparisonOp.LESS;
    case GREATER_EQUAL:
      return KuduPredicate.ComparisonOp.LESS_EQUAL;
    case LESS:
      return KuduPredicate.ComparisonOp.GREATER;
    case LESS_EQUAL:
      return KuduPredicate.ComparisonOp.GREATER_EQUAL;
    case EQUAL:
      return currentOp;
    default:
      throw new IllegalArgumentException(
          String.format("Passed in an Operator that doesn't make sense for Kudu Predicates: %s", currentOp));
    }
  }
}
