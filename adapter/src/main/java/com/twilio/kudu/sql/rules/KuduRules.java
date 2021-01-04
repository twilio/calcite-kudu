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

import java.util.Arrays;
import java.util.List;

public class KuduRules {

  public static final KuduFilterRule FILTER = new KuduFilterRule(RelFactories.LOGICAL_BUILDER);
  public static final KuduProjectRule PROJECT = new KuduProjectRule(RelFactories.LOGICAL_BUILDER);
  public static final RelOptRule SORT = KuduSortRule.INSTANCE;
  public static final KuduLimitRule LIMIT = new KuduLimitRule();
  public static final RelOptRule SORT_OVER_JOIN_TRANSPOSE = new SortInnerJoinTranspose(RelFactories.LOGICAL_BUILDER);
  public static final KuduNestedJoinRule NESTED_JOIN = new KuduNestedJoinRule.KuduNestedOverFilter(
      RelFactories.LOGICAL_BUILDER);
  public static final KuduNestedJoinRule NESTED_JOIN_OVER_SORT = new KuduNestedJoinRule.KuduNestedOverSortAndFilter(
      RelFactories.LOGICAL_BUILDER);
  public static final KuduNestedJoinRule NESTED_JOIN_OVER_LIMIT = new KuduNestedJoinRule.KuduNestedOverSortAndFilter(
      RelFactories.LOGICAL_BUILDER);
  public static final KuduNestedJoinRule NESTED_JOIN_OVER_LIMIT_SORT_FILTER = new KuduNestedJoinRule.KuduNestedOverLimitAndSortAndFilter(
      RelFactories.LOGICAL_BUILDER);

  public static List<RelOptRule> RULES = Arrays.asList(FILTER, PROJECT, SORT, LIMIT, SORT_OVER_JOIN_TRANSPOSE,
      NESTED_JOIN, NESTED_JOIN_OVER_SORT, KuduSortedAggregationRule.SORTED_AGGREGATION_RULE,
      KuduSortedAggregationRule.SORTED_AGGREGATION_LIMIT_RULE, NESTED_JOIN_OVER_LIMIT,
      NESTED_JOIN_OVER_LIMIT_SORT_FILTER, KuduToEnumerableConverter.INSTANCE);
}
