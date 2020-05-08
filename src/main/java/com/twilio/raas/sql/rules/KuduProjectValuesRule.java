package com.twilio.raas.sql.rules;

import com.google.common.collect.ImmutableList;
import com.twilio.raas.sql.KuduRelNode;
import com.twilio.raas.sql.rel.KuduProjectValuesRel;
import com.twilio.raas.sql.rel.KuduValuesRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

public class KuduProjectValuesRule extends ConverterRule {

  public static final KuduProjectValuesRule INSTANCE = new KuduProjectValuesRule();

  private KuduProjectValuesRule() {
    super(LogicalProject.class, Convention.NONE, KuduRelNode.CONVENTION, "KuduProjectValuesRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Project project = (Project) rel;
    boolean containsOnlyLiterals = true;
    boolean containsOnlyBinds = true;
    for (RexNode node :  project.getProjects()) {
      if (!(node instanceof  RexLiteral)) {
        containsOnlyLiterals = false;
      }
      else if (!(node instanceof RexDynamicParam)) {
        containsOnlyBinds = false;
      }
    }
    if (containsOnlyLiterals) {
      // create KuduValuesRel that uses the literals
      ImmutableList<RexLiteral> tuples =
        project.getProjects().stream()
          .map(node -> (RexLiteral) node)
          .collect(ImmutableList.toImmutableList());
      return new KuduValuesRel(
        project.getCluster(),
        project.getRowType(),
        ImmutableList.of(tuples),
        project.getTraitSet().replace(KuduRelNode.CONVENTION));
    }
    else if (containsOnlyBinds) {
      // create a KuduProjectValuesRel that sets the number of bind expressions
      return new KuduProjectValuesRel(project.getCluster(),
        project.getTraitSet().replace(KuduRelNode.CONVENTION),
        convert(project.getInput(), KuduRelNode.CONVENTION),
        project.getProjects(),
        project.getRowType());
    }
    else {
      throw new IllegalArgumentException("INSERT statement only supports constant literals or bind parameters");
    }
  }

}
