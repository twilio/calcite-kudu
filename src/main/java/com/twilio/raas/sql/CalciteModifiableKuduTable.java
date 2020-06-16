package com.twilio.raas.sql;

import com.twilio.kudu.metadata.CubeTableInfo;
import com.twilio.raas.sql.mutation.CubeMaintainer;
import com.twilio.raas.sql.mutation.CubeMutationState;
import com.twilio.raas.sql.mutation.MutationState;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;

import java.util.Collection;
import java.util.List;

public class CalciteModifiableKuduTable extends CalciteKuduTable implements ModifiableTable {

  // used to keep track of state required to write rows to the kudu table
  protected final MutationState mutationState;

  // used to compute the aggregated values that will be written to the kudu cube table
  private CubeMaintainer cubeMaintainer;

  /**
   * Create the {@code CalciteKuduTable} for a physical scan over the provided{@link KuduTable}.
   * {@code KuduTable} must exist and be opened.
   *
   * Use {@link CalciteKuduTableBuilder} to build a table instead of this constructor
   *
   * @param kuduTable                    the opened kudu table
   * @param client                       kudu client that is used to write mutations
   * @param descendingOrderColumnIndexes indexes of columns that are stored in descending ordere
   * @param cubeTables                   the list of cube tables (if this is a fact table)
   * @param tableType                    type of this table
   */
  CalciteModifiableKuduTable(final KuduTable kuduTable, final AsyncKuduClient client,
                                     final List<Integer> descendingOrderColumnIndexes,
                                     final int timestampColumnIndex, final List<CalciteKuduTable> cubeTables,
                                     final TableType tableType, final CubeTableInfo.EventTimeAggregationType eventTimeAggregationType) {
    super(kuduTable, client, descendingOrderColumnIndexes, timestampColumnIndex, cubeTables,
      tableType, eventTimeAggregationType);
    this.mutationState = tableType == TableType.CUBE ? new CubeMutationState(this) :
      new MutationState(this);
  }

  @Override
  public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table,
                                       Prepare.CatalogReader catalogReader, RelNode child,
                                       TableModify.Operation operation,
                                       List<String> updateColumnList,
                                       List<RexNode> sourceExpressionList, boolean flattened) {
    if (!operation.equals(TableModify.Operation.INSERT)) {
      throw new UnsupportedOperationException("Only INSERT statement is supported");
    } else if (tableType == TableType.CUBE) {
      throw new UnsupportedOperationException("Cannot directly write to a cube table");
    }
    return new KuduWrite(
      this.kuduTable,
      cluster,
      table,
      catalogReader,
      child,
      operation,
      updateColumnList,
      sourceExpressionList,
      flattened);
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

  public MutationState getMutationState() {
    return mutationState;
  }

  /**
   * Called while using {@link java.sql.Statement}
   */
  public int mutateTuples(final List<Integer> columnIndexes,
                          final List<List<RexLiteral>> tuples) {
    return getMutationState().mutateTuples(columnIndexes, tuples);
  }

  /**
   * Called while using {@link java.sql.PreparedStatement}
   */
  public int mutateRow(final List<Integer> columnIndexes, final List<Object> values) {
    return getMutationState().mutateRow(columnIndexes, values);
  }
}
