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

import com.twilio.kudu.sql.CalciteKuduPredicate;
import com.twilio.kudu.sql.KuduQuery;
import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.rel.KuduFilterRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class KuduFilterRule extends RelOptRule {
  public KuduFilterRule(RelBuilderFactory relBuilderFactory) {
    super(operand(LogicalFilter.class, operand(KuduQuery.class, none())), relBuilderFactory, "KuduPushDownFilters");
  }

  /**
   * When we match, this method needs to transform the {@link RelOptRuleCall} into
   * a new rule with push filters applied.
   */
  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = (LogicalFilter) call.getRelList().get(0);
    final KuduQuery kuduQuery = (KuduQuery) call.getRelList().get(1);
    if (filter.getTraitSet().contains(Convention.NONE)) {
      final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
      // expand row value expression into a series of OR-AND expressions
      RowValueExpressionConverter visitor = new RowValueExpressionConverter(rexBuilder, kuduQuery.calciteKuduTable);
      final RexNode condition = filter.getCondition().accept(visitor);
      final KuduPredicatePushDownVisitor predicateParser = new KuduPredicatePushDownVisitor(rexBuilder);

      List<List<CalciteKuduPredicate>> predicates = condition.accept(predicateParser, null);
      if (predicates.isEmpty()) {
        // if we could not handle any of the filters in Kudu, just return and let
        // Calcite
        // handle filtering
        return;
      }
      final RelNode converted = new KuduFilterRel(filter.getCluster(),
          filter.getTraitSet().replace(KuduRelNode.CONVENTION), convert(filter.getInput(), KuduRelNode.CONVENTION),
          filter.getCondition(), predicates, kuduQuery.calciteKuduTable.getKuduTable().getSchema(),
          !predicateParser.areAllFiltersApplied());

      call.transformTo(converted);
    }
  }

}
