/* Copyright 2021 Twilio, Inc
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

import java.util.List;

import org.apache.calcite.linq4j.Enumerable;

/**
 * An {@link Enumerable} that can be cloned with additional conjunctions.
 */
public interface CloneableEnumerable<T> extends Enumerable<T> {
  /**
   * Clone this enumerable by merging the {@link CalciteKuduPredicate} into
   * existing set.
   *
   * @param conjunctions the additional predicates that need to be conjoined to
   *                     the query.
   *
   * @return another {@code CloneableEnumerable} with the additional conjunctions
   */
  public CloneableEnumerable<T> clone(final List<List<CalciteKuduPredicate>> conjunctions);
}
