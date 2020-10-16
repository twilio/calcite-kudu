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

import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.rel.KuduToEnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Predicates;
import org.slf4j.Logger;

/**
 * Rule to convert a relational expression from {@link KuduRelNode#CONVENTION}
 * to {@link EnumerableConvention}.
 *
 * Bulk of the work is in the implement method which takes an
 * {@link EnumerableRelImplementor} and creates
 * {@link org.apache.calcite.linq4j.tree.BlockStatement}.
 */
public class KuduToEnumerableConverter extends ConverterRule {

  public static final Logger logger = CalciteTrace.getPlannerTracer();

  public static final ConverterRule INSTANCE = new KuduToEnumerableConverter(RelFactories.LOGICAL_BUILDER);

  private KuduToEnumerableConverter(RelBuilderFactory relBuilderFactory) {
    super(RelNode.class, Predicates.<RelNode>alwaysTrue(), KuduRelNode.CONVENTION, EnumerableConvention.INSTANCE,
        relBuilderFactory, "KuduToEnumerableConverterRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
    return new KuduToEnumerableRel(rel.getCluster(), newTraitSet, rel);
  }

}
