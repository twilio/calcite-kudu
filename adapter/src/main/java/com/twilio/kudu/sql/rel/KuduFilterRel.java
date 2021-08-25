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
package com.twilio.kudu.sql.rel;

import java.util.List;

import com.twilio.kudu.sql.CalciteKuduPredicate;
import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.rel.KuduProjectRel.KuduColumnVisitor;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.kudu.Schema;

public class KuduFilterRel extends Filter implements KuduRelNode {
  public final List<List<CalciteKuduPredicate>> scanPredicates;
  public final Schema kuduSchema;
  public final boolean useInMemoryFiltering;
  private final double totalTablets;
  private final double filteredTablets;

  public KuduFilterRel(final RelOptCluster cluster, final RelTraitSet traitSet, final RelNode child,
      final RexNode condition, final List<List<CalciteKuduPredicate>> predicates, final Schema kuduSchema,
      final boolean useInMemoryFiltering, final double totalTablets, final double filteredTablets) {
    super(cluster, traitSet, child, condition);
    this.scanPredicates = predicates;
    this.kuduSchema = kuduSchema;
    this.useInMemoryFiltering = useInMemoryFiltering;
    this.totalTablets = totalTablets;
    this.filteredTablets = filteredTablets;
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
    // totalTablets is the number of tablets for the table
    // filteredTablets is the number of tablets that are removed from this filter

    if (filteredTablets == totalTablets) {
      return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }
    final double tabletFilterPercentage = filteredTablets / totalTablets;
    return super.computeSelfCost(planner, mq).multiplyBy(tabletFilterPercentage);
  }

  public KuduFilterRel copy(final RelTraitSet traitSet, final RelNode input, final RexNode condition) {
    return new KuduFilterRel(getCluster(), traitSet, input, condition, this.scanPredicates, kuduSchema,
        useInMemoryFiltering, totalTablets, filteredTablets);
  }

  @Override
  public void implement(final Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.predicates.addAll(this.scanPredicates);

    if (useInMemoryFiltering) {
      final KuduColumnVisitor columnExtractor = new KuduColumnVisitor();
      implementor.inMemoryCondition = getCondition();
      implementor.filterProjections = getCondition().accept(columnExtractor);
    }
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw) {
    pw.input("input", getInput());
    int scanCount = 1;
    for (final List<CalciteKuduPredicate> scanPredicate : scanPredicates) {
      final StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (final CalciteKuduPredicate predicate : scanPredicate) {

        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }

        sb.append(predicate.explainPredicate(kuduSchema.getColumnByIndex(predicate.getColumnIdx())));
      }
      pw.item("ScanToken " + scanCount++, sb.toString());
    }
    if (useInMemoryFiltering) {
      pw.item("MemoryFilters", getCondition());
    }
    return pw;
  }
}
