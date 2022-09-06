/* Copyright 2022 Twilio, Inc
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
package com.twilio.kudu.sql.metadata;

import com.google.common.collect.Range;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

public class KuduRelMdSelectivity extends RelMdSelectivity {

  private static final KuduRelMdSelectivity INSTANCE = new KuduRelMdSelectivity();
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.SELECTIVITY.method, INSTANCE);

  // TODO is there a way to set this based on the time range of the query
  private static final double MAX_TIME_RANGE = DateTimeUtils.MILLIS_PER_DAY * 365d;

  public Double getSelectivity(RelNode rel, RelMetadataQuery mq, @Nullable RexNode predicate) {
    if (predicate == null) {
      return RelMdUtil.guessSelectivity(predicate);
    }
    final RexExecutor executor = Util.first(rel.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RexSimplify rexSimplify = new RexSimplify(rel.getCluster().getRexBuilder(), RelOptPredicateList.EMPTY,
        executor);
    RexNode simplifiedPred = rexSimplify.simplify(predicate);
    double sel = 1.0;
    for (RexNode pred : RelOptUtil.conjunctions(simplifiedPred)) {
      if (pred.getKind() == SqlKind.SEARCH) {
        RexCall call = (RexCall) pred;
        Sarg sarg = ((RexLiteral) call.getOperands().get(1)).getValueAs(Sarg.class);
        double rangSelectivity = 0.d;
        for (Object o : sarg.rangeSet.asRanges()) {
          Range r = (Range) o;
          if (!r.hasLowerBound() || !r.hasUpperBound() || !(r.lowerEndpoint() instanceof TimestampString)
              || !(r.upperEndpoint() instanceof TimestampString)) {
            sel *= RelMdUtil.guessSelectivity(predicate);
          } else {
            long lowerBound = r.hasLowerBound() ? ((TimestampString) r.lowerEndpoint()).getMillisSinceEpoch() : 0l;
            long upperBound = r.hasUpperBound() ? ((TimestampString) r.upperEndpoint()).getMillisSinceEpoch()
                : System.currentTimeMillis();
            rangSelectivity += (upperBound - lowerBound) / MAX_TIME_RANGE;
            rangSelectivity = Math.min(1.0, rangSelectivity);
            sel *= rangSelectivity;
          }
        }
      } else {
        sel *= RelMdUtil.guessSelectivity(predicate);
      }
    }
    return sel;
  }
}
