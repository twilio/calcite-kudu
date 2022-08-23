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
package com.twilio.kudu.sql.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.materialize.KuduMaterializedViewOnlyAggregateRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyAggregateRule;

import java.util.Arrays;
import java.util.List;

public class KuduRules {

  public static final KuduFilterRule FILTER = new KuduFilterRule(RelFactories.LOGICAL_BUILDER);
  public static final KuduProjectRule PROJECT = new KuduProjectRule(RelFactories.LOGICAL_BUILDER);
  public static final RelOptRule FILTER_SORT = KuduSortRule.FILTER_SORT_RULE;
  public static final RelOptRule SORT = KuduSortRule.SIMPLE_SORT_RULE;
  public static final KuduLimitRule LIMIT = new KuduLimitRule();
  public static final RelOptRule SORT_OVER_JOIN_TRANSPOSE = new SortInnerJoinTranspose(RelFactories.LOGICAL_BUILDER);
  public static final KuduNestedJoinRule NESTED_JOIN = new KuduNestedJoinRule(RelFactories.LOGICAL_BUILDER);
  public static final RelOptRule MV_AGGREGATE = new KuduMaterializedViewOnlyAggregateRule(
      MaterializedViewOnlyAggregateRule.Config.DEFAULT);

  public static List<RelOptRule> ENUMERABLE_RULES = Arrays.asList(NESTED_JOIN, KuduToEnumerableConverter.INSTANCE);
  public static List<RelOptRule> CORE_RULES = Arrays.asList(FILTER, PROJECT, SORT, FILTER_SORT, LIMIT,
      SORT_OVER_JOIN_TRANSPOSE, KuduSortedAggregationRule.SORTED_AGGREGATION_RULE,
      KuduAggregationLimitRule.AGGREGATION_LIMIT_RULE, KuduFilterIntoJoinRule.KUDU_FILTER_INTO_JOIN);
}
