package com.twilio.raas.sql;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import org.apache.calcite.linq4j.tree.Types;
import java.util.List;


/**
 * Builtin methods in the KuduDB adapter.
 */
public enum KuduMethod {
    KUDU_QUERY_METHOD(CalciteKuduTable.KuduQueryable.class, "query", List.class,
            List.class, int.class, int.class, boolean.class);

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
