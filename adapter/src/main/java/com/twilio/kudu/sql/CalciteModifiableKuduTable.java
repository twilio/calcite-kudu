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

import com.twilio.kudu.sql.metadata.CubeTableInfo;
import com.twilio.kudu.sql.mutation.CubeMaintainer;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;

import java.util.Collection;
import java.util.List;

public class CalciteModifiableKuduTable extends CalciteKuduTable implements ModifiableTable {

  // used to compute the aggregated values that will be written to the kudu cube
  // table
  private CubeMaintainer cubeMaintainer;

  private final boolean disableCubeAggregations;

  /**
   * Create the {@code CalciteKuduTable} for a physical scan over the
   * provided{@link KuduTable}. {@code KuduTable} must exist and be opened.
   *
   * Use {@link CalciteKuduTableBuilder} to build a table instead of this
   * constructor
   *
   * @param kuduTable                    the opened kudu table
   * @param client                       kudu client that is used to write
   *                                     mutations
   * @param descendingOrderColumnIndexes indexes of columns that are stored in
   *                                     descending ordere
   * @param cubeTables                   the list of cube tables (if this is a
   *                                     fact table)
   * @param tableType                    type of this table
   */
  CalciteModifiableKuduTable(final KuduTable kuduTable, final AsyncKuduClient client,
      final List<Integer> descendingOrderColumnIndexes, final int timestampColumnIndex,
      final List<CalciteKuduTable> cubeTables, final TableType tableType,
      final CubeTableInfo.EventTimeAggregationType eventTimeAggregationType, final long readSnapshotTimeDifference,
      final boolean disableCubeAggregations) {
    super(kuduTable, client, descendingOrderColumnIndexes, timestampColumnIndex, cubeTables, tableType,
        eventTimeAggregationType, readSnapshotTimeDifference);
    this.disableCubeAggregations = disableCubeAggregations;
  }

  @Override
  public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader,
      RelNode child, TableModify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList,
      boolean flattened) {
    if (!operation.equals(TableModify.Operation.INSERT)) {
      throw new UnsupportedOperationException("Only INSERT statement is supported");
    } else if (tableType == TableType.CUBE) {
      throw new UnsupportedOperationException("Cannot directly write to a cube table");
    }
    return new KuduWrite(this.kuduTable, cluster, table, catalogReader, child, operation, updateColumnList,
        sourceExpressionList, flattened);
  }

  @Override
  public Collection getModifiableCollection() {
    return null;
  }

  public void createCubeMaintainer(final CalciteKuduTable factTable) {
    if (tableType == TableType.CUBE) {
      cubeMaintainer = new CubeMaintainer(this, factTable);
    }
  }

  public CubeMaintainer getCubeMaintainer() {
    return cubeMaintainer;
  }

  public boolean isDisableCubeAggregations() {
    return disableCubeAggregations;
  }
}
