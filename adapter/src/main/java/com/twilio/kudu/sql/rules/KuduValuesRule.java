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
import com.twilio.kudu.sql.rel.KuduValuesRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;

public class KuduValuesRule extends ConverterRule {
  public static final KuduValuesRule INSTANCE = new KuduValuesRule();

  private KuduValuesRule() {
    super(LogicalValues.class, Convention.NONE, KuduRelNode.CONVENTION, "KuduValuesRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Values values = (Values) rel;
    return new KuduValuesRel(values.getCluster(), values.getRowType(), values.getTuples(),
        values.getTraitSet().replace(KuduRelNode.CONVENTION));
  }
}