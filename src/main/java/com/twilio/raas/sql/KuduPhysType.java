package com.twilio.raas.sql;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.RowResult;

public final class KuduPhysType implements PhysType {
    private static Method LONG_METHOD = Types.lookupMethod(RowResult.class, "getLong", int.class);
    private static Method STRING_METHOD = Types.lookupMethod(RowResult.class, "getString", int.class);
    private static Method INT_METHOD = Types.lookupMethod(RowResult.class, "getInt", int.class);
    private static Method SHORT_METHOD = Types.lookupMethod(RowResult.class, "getShort", int.class);
    private static Method BYTE_METHOD = Types.lookupMethod(RowResult.class, "getByte", int.class);
    private static Method BOOL_METHOD = Types.lookupMethod(RowResult.class, "getBoolean", int.class);
    private static Method FLOAT_METHOD = Types.lookupMethod(RowResult.class, "getFloat", int.class);
    private static Method DOUBLE_METHOD = Types.lookupMethod(RowResult.class, "getDouble", int.class);
    private static Method DECIMAL_METHOD = Types.lookupMethod(RowResult.class, "getDecimal", int.class);
    private static Method BINARY_METHOD = Types.lookupMethod(RowResult.class, "getBinary", int.class);

    private static Method TIMESTAMP_METHOD = Types.lookupMethod(RowResult.class, "getTimestamp", int.class);
    private static Method TO_INSTANT = Types.lookupMethod(Timestamp.class, "toInstant");
    private static Method TO_EPOCH_MS = Types.lookupMethod(Instant.class, "toEpochMilli");

    private static Method BYTE_ARRAY = Types.lookupMethod(ByteBuffer.class, "array");

    private final Schema tableSchema;
    private final RelDataType logicalType;
    private final List<Integer> descendingSortedFieldIndices;
    private final List<Integer> kuduColumnProjections;

    public KuduPhysType(final Schema tableSchema, final RelDataType logicalType,  final List<Integer> descendingSortedFieldIndices, final List<Integer> kuduColumnProjections) {
        this.tableSchema = tableSchema;
        this.logicalType = logicalType;
        this.descendingSortedFieldIndices = descendingSortedFieldIndices;
        this.kuduColumnProjections = kuduColumnProjections;
    }

    @Override
    public Type getJavaRowType() {
        return RowResult.class;
    }
    @Override
    public Type getJavaFieldType(int field) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public PhysType field(int ordinal) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public PhysType component(int field) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public RelDataType getRowType() {
        return logicalType;
    }
    @Override
    public Class fieldClass(int field) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public boolean fieldNullable(int index) {
        // TODO Auto-generated method stub
        return false;
    }
    @Override
    public Expression fieldReference(Expression expression, int field) {
        // TODO Auto-generated method stub
        return fieldReference(expression, field, null);
    }
    @Override
    public Expression fieldReference(Expression expression, int ord, Type storageType) {
        final int field = this.kuduColumnProjections.indexOf(ord);
        final ColumnSchema columnSchema = tableSchema.getColumns().get(ord);
        Expression descendingSortMax = null;
        final Expression rawFetch;
        final Expression columnRef = Expressions.constant(field);
        switch(columnSchema.getType()) {
        case INT8:
            rawFetch = Expressions.call(expression, BYTE_METHOD, columnRef);
            descendingSortMax = Expressions.constant(Byte.MAX_VALUE, Byte.class);
            break;
        case INT16:
            rawFetch = Expressions.call(expression, SHORT_METHOD, columnRef);
            descendingSortMax = Expressions.constant(Short.MAX_VALUE, Short.class);
            break;
        case INT32:
            rawFetch = Expressions.call(expression, INT_METHOD, columnRef);
            descendingSortMax = Expressions.constant(Integer.MAX_VALUE, Integer.class);
            break;
        case INT64:
            rawFetch = Expressions.call(expression, LONG_METHOD, columnRef);
            descendingSortMax = Expressions.constant(Long.MAX_VALUE, Long.class);
            break;
        case UNIXTIME_MICROS:
            final Expression timestamp = Expressions.call(expression, TIMESTAMP_METHOD, columnRef);
            final Expression instantObj = Expressions.call(timestamp, TO_INSTANT);
            final Expression millis = Expressions.call(instantObj, TO_EPOCH_MS);
            rawFetch = millis;
            descendingSortMax = Expressions.constant(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS,
                    Long.class);
            break;

        case STRING:
            rawFetch = Expressions.call(expression, STRING_METHOD, columnRef);
            break;
        case BOOL:
            rawFetch = Expressions.call(expression, BOOL_METHOD, columnRef);
            break;
        case FLOAT:
            rawFetch = Expressions.call(expression, FLOAT_METHOD, columnRef);
            break;
        case DOUBLE:
            rawFetch = Expressions.call(expression, DOUBLE_METHOD, columnRef);
            break;
        case DECIMAL:
            rawFetch = Expressions.call(expression, DECIMAL_METHOD, columnRef);
            break;
        case BINARY:
            final Expression byteBuffer = Expressions.call(expression, BINARY_METHOD, columnRef);
            // @TODO: is this buffer readOnly?
            final Expression getArray = Expressions.call(byteBuffer, BYTE_ARRAY);
            rawFetch = getArray;
            break;
        default:
            throw new IllegalArgumentException("Unable to do the thing " + columnSchema.getType());
        }

        if (descendingSortedFieldIndices.contains(ord)) {
            if (descendingSortMax == null) {
                throw new IllegalStateException(String.format("Ord %d is of type %s and cannot be descending sorted",
                        field, columnSchema));
            }
            return Expressions.subtract(descendingSortMax, rawFetch);
        }
        else {
            return rawFetch;
        }

    }
    @Override
    public Expression generateAccessor(List<Integer> fields) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Expression generateSelector(ParameterExpression parameter, List<Integer> fields) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Expression generateSelector(ParameterExpression parameter, List<Integer> fields,
        JavaRowFormat targetFormat) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Expression generateSelector(ParameterExpression parameter, List<Integer> fields, List<Integer> usedFields,
        JavaRowFormat targetFormat) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Pair<Type, List<Expression>> selector(ParameterExpression parameter, List<Integer> fields,
        JavaRowFormat targetFormat) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public PhysType project(List<Integer> integers, JavaRowFormat format) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public PhysType project(List<Integer> integers, boolean indicator, JavaRowFormat format) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Pair<Expression, Expression> generateCollationKey(List<RelFieldCollation> collations) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Expression generateComparator(RelCollation collation) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Expression comparer() {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Expression record(List<Expression> expressions) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public JavaRowFormat getFormat() {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public List<Expression> accessors(Expression parameter, List<Integer> argList) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public PhysType makeNullable(boolean nullable) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Expression convertTo(Expression expression, PhysType targetPhysType) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public Expression convertTo(Expression expression, JavaRowFormat targetFormat) {
        // TODO Auto-generated method stub
        return null;
    }
}
