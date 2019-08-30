package com.twilio.raas.sql.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;

import java.util.Arrays;
import java.util.List;

public class KuduRules {

    public static final KuduFilterRule FILTER = new KuduFilterRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduProjectRule PROJECT = new KuduProjectRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduSortWithFilterRule FILTER_SORT =
            new KuduSortWithFilterRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduSortWithoutFilterRule SORT =
            new KuduSortWithoutFilterRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduLimitRule LIMIT = new KuduLimitRule();

    public static List<RelOptRule> RULES = Arrays.asList(
            FILTER,
            PROJECT,
            SORT,
            FILTER_SORT,
            LIMIT
    );

}
