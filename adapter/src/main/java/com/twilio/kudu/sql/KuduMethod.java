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

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;

import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.core.Join;

/**
 * Builtin methods in the KuduDB adapter.
 */
public enum KuduMethod {
  KUDU_QUERY_METHOD(CalciteKuduTable.KuduQueryable.class, "query", List.class, List.class, int.class, int.class,
      boolean.class, boolean.class, KuduScanStats.class, AtomicBoolean.class, Function1.class, Predicate1.class,
      boolean.class),
  KUDU_MUTATE_TUPLES_METHOD(CalciteKuduTable.KuduQueryable.class, "mutateTuples", List.class, List.class),
  KUDU_MUTATE_ROW_METHOD(CalciteKuduTable.KuduQueryable.class, "mutateRow", List.class, List.class),
  NESTED_JOIN_PREDICATES(KuduEnumerable.class, "nestedJoinPredicates", Join.class, EqualityComparer.class,
      Predicate2.class);

  public final Method method;

  public static final ImmutableMap<Method, KuduMethod> MAP;

  static {
    final ImmutableMap.Builder<Method, KuduMethod> builder = ImmutableMap.builder();
    for (KuduMethod value : KuduMethod.values()) {
      builder.put(value.method, value);
    }
    MAP = builder.build();
  }

  KuduMethod(Class clazz, String methodName, Class... argumentTypes) {
    this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
  }
}
