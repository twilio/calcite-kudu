package com.twilio.raas.sql;

import com.twilio.raas.sql.rules.KuduProjectValuesRule;
import com.twilio.raas.sql.rules.KuduToEnumerableConverter;
import com.twilio.raas.sql.rules.KuduValuesRule;
import com.twilio.raas.sql.rules.KuduWriteRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.kudu.client.KuduTable;

import java.util.List;

public class KuduWrite extends TableModify implements KuduRelNode {

  private KuduTable kuduTable;

  public KuduWrite(
    KuduTable kuduTable,
    RelOptCluster cluster,
    RelOptTable table,
    Prepare.CatalogReader catalogReader,
    RelNode child,
    Operation operation,
    List<String> updateColumnList,
    List<RexNode> sourceExpressionList,
    boolean flattened) {
    super(
      cluster,
      cluster.traitSetOf(KuduRelNode.CONVENTION),
      table,
      catalogReader,
      child,
      operation,
      updateColumnList,
      sourceExpressionList,
      flattened);
    this.kuduTable = kuduTable;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    KuduWrite newRel =
      new KuduWrite(
        kuduTable,
        getCluster(),
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getOperation(),
        getUpdateColumnList(),
        getSourceExpressionList(),
        isFlattened());
    newRel.traitSet = traitSet;
    return newRel;
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.clear();
    planner.addRule(KuduProjectValuesRule.INSTANCE);
    planner.addRule(KuduValuesRule.INSTANCE);
    planner.addRule(KuduWriteRule.INSTANCE);
    planner.addRule(KuduToEnumerableConverter.INSTANCE);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.table = table;
    implementor.kuduTable = kuduTable;
  }

}