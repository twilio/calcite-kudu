package com.twilio.raas.sql;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexLiteral;

/**
 * Builtin methods in the KuduDB adapter.
 */
public enum KuduMethod {
    KUDU_QUERY_METHOD(CalciteKuduTable.KuduQueryable.class, "query", List.class,
        List.class, int.class, int.class, boolean.class, boolean.class, KuduScanStats.class, AtomicBoolean.class),
    KUDU_MUTATE_TUPLES_METHOD(CalciteKuduTable.KuduQueryable.class, "mutateTuples", List.class,
      List.class),
    KUDU_MUTATE_ROW_METHOD(CalciteKuduTable.KuduQueryable.class, "mutateRow", List.class,
      List.class),
    NESTED_JOIN_PREDICATES(KuduEnumerable.class, "nestedJoinPredicates", Join.class);

    public final Method method;

    public static final ImmutableMap<Method, KuduMethod> MAP;

    static {
        final ImmutableMap.Builder<Method, KuduMethod> builder =
            ImmutableMap.builder();
        for (KuduMethod value : KuduMethod.values()) {
            builder.put(value.method, value);
        }
        MAP = builder.build();
    }

    KuduMethod(Class clazz, String methodName, Class... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }
}
