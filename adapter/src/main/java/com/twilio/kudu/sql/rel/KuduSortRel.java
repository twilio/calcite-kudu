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

import com.google.common.collect.Lists;
import com.twilio.kudu.sql.KuduRelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import java.util.List;

/**
 * This relation sets {@link Implementor#sorted} to true and conditionally sets
 * {@link Implementor#limit} and {@link Implementor#offset}.
 */
public class KuduSortRel extends Sort implements KuduRelNode {

  public static final Logger LOGGER = CalciteTrace.getPlannerTracer();
  public final boolean groupBySorted;
  // If KuduAggregationLimitRule matched then the columns being sorted are a
  // prefix of the PK
  // columns and additional columns as well so we need to sort the returned rows.
  // sortPrefixColumns contains the PK prefix columns
  // If any of the subclasses of KuduSortRule matched then the columns being
  // sorted
  // are a prefix of the PK columns so we do not need to sort the returned rows
  // and
  // sortPrefixColumns is empty.
  public final List<RelFieldCollation> sortPkPrefixColumns;
  public final List<String> sortPkColumns;

  public KuduSortRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation, RexNode offset,
      RexNode fetch, List<String> sortPkColumns) {
    this(cluster, traitSet, child, collation, offset, fetch, false, Lists.newArrayList(), sortPkColumns);
  }

  public KuduSortRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RelCollation collation, RexNode offset,
      RexNode fetch, boolean groupBySorted, List<RelFieldCollation> sortPkPrefixColumns, List<String> sortPkColumns) {
    super(cluster, traitSet, child, collation, offset, fetch);
    assert getConvention() == KuduRelNode.CONVENTION;
    assert getConvention() == child.getConvention();
    this.groupBySorted = groupBySorted;
    this.sortPkPrefixColumns = sortPkPrefixColumns;
    this.sortPkColumns = sortPkColumns;
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new KuduSortRel(getCluster(), traitSet, input, collation, offset, fetch, groupBySorted, sortPkPrefixColumns,
        sortPkColumns);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.item("groupBySorted", groupBySorted);
    if (!sortPkPrefixColumns.isEmpty()) {
      pw.item("sortPkPrefixColumns", sortPkPrefixColumns);
    }
    return pw;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    double dRows = Double.MIN_VALUE;
    double dCpu = 0;
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    // create a sorted enumerator
    implementor.sorted = true;
    // set the offset
    if (offset != null) {
      final RexLiteral parsedOffset = (RexLiteral) offset;
      final Long properOffset = (Long) parsedOffset.getValue2();
      implementor.offset = properOffset;
    }
    // set the limit
    if (fetch != null) {
      final RexLiteral parsedFetch = (RexLiteral) fetch;
      final Long properFetch = (Long) parsedFetch.getValue2();
      implementor.limit = properFetch;
    }

    implementor.groupByLimited = groupBySorted;
    implementor.sortPkPrefixColumns = sortPkPrefixColumns;
    implementor.sortPkColumns = sortPkColumns;
  }
}
