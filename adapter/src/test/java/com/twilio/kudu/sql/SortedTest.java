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
package com.twilio.kudu.sql;

import com.google.common.collect.Lists;
import com.twilio.kudu.sql.rel.KuduToEnumerableRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public final class SortedTest {

  @Test
  public void findPrimaryKeyOrder() {
    final KuduToEnumerableRel kuduToEnumerableRel = new KuduToEnumerableRel(mock(RelOptCluster.class),
        mock(RelTraitSet.class), mock(RelNode.class));
    assertEquals("Expected to find just account_id from projection", Arrays.asList(1),
        kuduToEnumerableRel.getPrimaryKeyColumnsInProjection(Lists.newArrayList(0), Arrays.asList(10, 0)));

    assertEquals("Expected to find account_id and date from projection", Arrays.asList(2, 1),
        kuduToEnumerableRel.getPrimaryKeyColumnsInProjection(Lists.newArrayList(0, 1), Arrays.asList(10, 1, 0)));

    assertEquals("Expected to find dateColumn from projection", Arrays.asList(1),
        kuduToEnumerableRel.getPrimaryKeyColumnsInProjection(Lists.newArrayList(1), Arrays.asList(10, 1)));
  }
}
