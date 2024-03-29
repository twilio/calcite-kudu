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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

public class KuduNonCumulativeCost implements MetadataHandler<BuiltInMetadata.NonCumulativeCost> {

  private static final KuduNonCumulativeCost INSTANCE = new KuduNonCumulativeCost();
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.NON_CUMULATIVE_COST.method, INSTANCE);

  @Override
  public MetadataDef<BuiltInMetadata.NonCumulativeCost> getDef() {
    return BuiltInMetadata.NonCumulativeCost.DEF;
  }

  public RelOptCost getNonCumulativeCost(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof Sort) {
      // increase the default cost of EnumerableLimitSort
      return rel.computeSelfCost(rel.getCluster().getPlanner(), mq).multiplyBy(1000);
    } else {
      return rel.computeSelfCost(rel.getCluster().getPlanner(), mq);
    }
  }
}
