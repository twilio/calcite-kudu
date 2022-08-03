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

import com.google.common.collect.ImmutableList;
import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.metadata.KuduRelMetadataProvider;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;

import java.util.stream.Collectors;

public class KuduValuesRel extends Values implements KuduRelNode {

  public KuduValuesRel(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples,
      RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
    // include our own metadata provider so that we can customize costs
    JaninoRelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(KuduRelMetadataProvider.INSTANCE);
    RelMetadataQuery.THREAD_PROVIDERS.set(relMetadataProvider);
    getCluster().setMetadataProvider(relMetadataProvider);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.columnIndexes = rowType.getFieldList().stream().map(RelDataTypeField::getIndex)
        .collect(Collectors.toList());
    implementor.tuples = tuples;
  }

}
