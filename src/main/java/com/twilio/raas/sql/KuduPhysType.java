package com.twilio.raas.sql;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.BuiltInMethod;
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
    public Type getJavaFieldType(final int field) {
        return fieldClass(field);
    }
    @Override
    public PhysType field(final int ordinal) {
        throw new UnsupportedOperationException();
    }
    @Override
    public PhysType component(final int field) {
        throw new UnsupportedOperationException();
    }
    @Override
    public RelDataType getRowType() {
        return logicalType;
    }
    @Override
    public Class fieldClass(final int field) {
        final ColumnSchema columnSchema = tableSchema.getColumns().get(field);
        switch(columnSchema.getType()) {
        case INT8:
            return Byte.class;
        case INT16:
            return Short.class;
        case INT32:
            return Integer.class;
        case INT64:
        case UNIXTIME_MICROS:
            return Long.class;
        case FLOAT:
            return Float.class;

        case DOUBLE:
            return Double.class;
        case DECIMAL:
            return BigDecimal.class;

        case STRING:
            return String.class;
        case BOOL:
            return Boolean.class;

        case BINARY:
            return byte[].class;
        default:
            throw new IllegalArgumentException("Unable to do the thing " + columnSchema.getType());
        }
    }
    @Override
    public boolean fieldNullable(final int index) {
        final ColumnSchema columnSchema = tableSchema.getColumns().get(index);
        return columnSchema.isNullable();
    }
    @Override
    public Expression fieldReference(final Expression expression, final int ord) {
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
    public Expression fieldReference(final Expression expression, final int ord, final Type storageType) {
        // @TODO: This probably should leverage storageType similar to
        // {@link org.apache.calcite.rex.RexLiteral#getValueAs(Class<T>)}
        return fieldReference(expression, ord);
    }
    @Override
    public Expression generateAccessor(final List<Integer> fields) {
        // Mostly Copy an pasta
        final ParameterExpression v1 =
        Expressions.parameter(getJavaRowType(), "v1");
        switch (fields.size()) {
        case 0:
            return Expressions.lambda(
                Function1.class,
                Expressions.field(
                    null,
                    BuiltInMethod.COMPARABLE_EMPTY_LIST.field),
          v1);
        case 1:
            final int field0 = fields.get(0);

            // new Function1<Employee, Res> {
            //    public Res apply(Employee v1) {
            //        return v1.<fieldN>;
            //    }
            // }
            final Class returnType = fieldClass(field0);
            final Expression fieldReference =
                EnumUtils.convert(
                    fieldReference(v1, field0),
                    returnType);
            return Expressions.lambda(
                Function1.class,
                fieldReference,
                v1);
        default:
            // new Function1<Employee, List> {
            //    public List apply(Employee v1) {
            //        return Arrays.asList(
            //            new Object[] {v1.<fieldN>, v1.<fieldM>});
            //    }
            // }
            final Expressions.FluentList<Expression> list = Expressions.list();
            for (final int field : fields) {
                list.add(fieldReference(v1, field));
            }
            switch (list.size()) {
            case 2:
                return Expressions.lambda(
                    Function1.class,
                    Expressions.call(
                        List.class,
                        null,
                        BuiltInMethod.LIST2.method,
                        list),
                    v1);
            case 3:
                return Expressions.lambda(
                    Function1.class,
                    Expressions.call(
                        List.class,
                        null,
                        BuiltInMethod.LIST3.method,
                  list),
                    v1);
            case 4:
                return Expressions.lambda(
                    Function1.class,
                    Expressions.call(
                        List.class,
                        null,
                        BuiltInMethod.LIST4.method,
                        list),
                    v1);
            case 5:
                return Expressions.lambda(
                    Function1.class,
                    Expressions.call(
                        List.class,
                        null,
                        BuiltInMethod.LIST5.method,
                        list),
                    v1);
            case 6:
                return Expressions.lambda(
                    Function1.class,
                    Expressions.call(
                        List.class,
                        null,
                        BuiltInMethod.LIST6.method,
                        list),
                    v1);
            default:
                return Expressions.lambda(
                    Function1.class,
                    Expressions.call(
                        List.class,
                        null,
                        BuiltInMethod.LIST_N.method,
                        Expressions.newArrayInit(
                            Comparable.class,
                            list)),
                    v1);
            }
        }
    }
    @Override
    public Expression generateSelector(final ParameterExpression parameter, final List<Integer> fields) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Expression generateSelector(final ParameterExpression parameter, final List<Integer> fields,
        final JavaRowFormat targetFormat) {
        return generateSelector(parameter, fields);
    }
    @Override
    public Expression generateSelector(final ParameterExpression parameter, final List<Integer> fields, final List<Integer> usedFields,
        final JavaRowFormat targetFormat) {
        return generateSelector(parameter, fields);
    }
    @Override
    public Pair<Type, List<Expression>> selector(final ParameterExpression parameter, final List<Integer> fields,
        final JavaRowFormat targetFormat) {
        throw new UnsupportedOperationException();
    }
    @Override
    public PhysType project(final List<Integer> integers, final JavaRowFormat format) {
        throw new UnsupportedOperationException();
    }
    @Override
    public PhysType project(final List<Integer> integers, final boolean indicator, final JavaRowFormat format) {
        return project(integers, format);
    }
    @Override
    public Pair<Expression, Expression> generateCollationKey(final List<RelFieldCollation> collations) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Expression generateComparator(final RelCollation collation) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Expression comparer() {
        throw new UnsupportedOperationException();
    }
    @Override
    public Expression record(final List<Expression> expressions) {
        throw new UnsupportedOperationException();
    }
    @Override
    public JavaRowFormat getFormat() {
        throw new UnsupportedOperationException();
    }
    @Override
    public List<Expression> accessors(final Expression parameter, final List<Integer> argList) {
        throw new UnsupportedOperationException();
    }
    @Override
    public PhysType makeNullable(final boolean nullable) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Expression convertTo(final Expression expression, final PhysType targetPhysType) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Expression convertTo(final Expression expression, final JavaRowFormat targetFormat) {
        throw new UnsupportedOperationException();
    }
}
