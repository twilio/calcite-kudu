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

import com.google.common.collect.ImmutableList;
import com.twilio.kudu.sql.rules.KuduToEnumerableConverter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.kudu.client.KuduTable;

/**
 * A relational expression that represents a Kudu convention This will be used
 * by the {@link KuduToEnumerableConverter} to translate into a {@link Blocks}.
 * The {@code Blocks} will be used to generate Byte code that represents the
 * query and executes it against the table.
 *
 * Each optimizer rule implemented in this module converts a boring basic
 * {@link RelNode} into a {@link KuduRelNode}.
 */
public interface KuduRelNode extends RelNode {

  Convention CONVENTION = new Convention.Impl("KUDU", KuduRelNode.class);

  /**
   * Each {@link KuduRelNode} implementation will accept the {@link Implementor}
   * and manipulate it. The {@code Implementor} will then be used by the
   * {@link KuduToEnumerableConverter} to create a {@link Blocks} that will be
   * used to generate Byte code for the actual query.
   *
   * @param implementor mutable implementator to store information on for the Kudu
   *                    RPCs
   */
  void implement(Implementor implementor);

  /**
   * Implementor is a container to hold information required to execute a query or
   * update to kudu. Each {@link KuduRelNode} implementation will add information
   * into this container.
   */
  class Implementor {
    public KuduTable kuduTable;
    public RelOptTable table;
    public List<RexNode> projections = Collections.emptyList();
    public RelDataType tableDataType;
    public List<Integer> descendingColumns = Collections.emptyList();
    public List<Integer> filterProjections = Collections.emptyList();
    public RexNode inMemoryCondition = null;

    // information required for executing a query
    public final List<Integer> kuduProjectedColumns = new ArrayList<>();
    public final List<List<CalciteKuduPredicate>> predicates = new ArrayList<>();
    public long limit = -1;
    public long offset = -1;
    public boolean sorted = false;
    public boolean groupByLimited = false;

    // information required for executing an update
    public List<Integer> columnIndexes;
    // list of tuples from a regular Statement
    public ImmutableList<ImmutableList<RexLiteral>> tuples;
    // number of column values to be bound from a PreparedStatement
    public int numBindExpressions;

    public void visitChild(int ordinal, RelNode input) {
      assert ordinal == 0;
      ((KuduRelNode) input).implement(this);
    }
  }
}
