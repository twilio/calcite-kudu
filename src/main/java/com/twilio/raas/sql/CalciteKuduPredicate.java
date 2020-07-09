package com.twilio.raas.sql;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.KuduPredicate;

import org.apache.kudu.Schema;

/**
 * A simple "case" class / POJO to help with code generation in
 * {@link com.twilio.raas.sql.rules.KuduToEnumerableConverter}.
 * Simplifies the {@link org.apache.calcite.linq4j.tree.Expression}
 * generation so it is more readable
 */
public abstract class CalciteKuduPredicate {
    /**
     * Transforms this POJO into a proper {@link KuduPredicate}
     *
     * @param columnSchema  column schema to use for the predicate
     * @param invertValue   true if the column is stored in descending order
     *
     * @return {@code KuduPredicate} that represents this POJO
     */
    public abstract KuduPredicate toPredicate(ColumnSchema columnSchema, boolean invertValue);

    /**
     * returns the column index for this predicate
     */
    public abstract int getColumnIdx();

    /**
     * Constructs a string used when generating the explain plan
     */
    public abstract String explainPredicate(final ColumnSchema columnSchema);

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
