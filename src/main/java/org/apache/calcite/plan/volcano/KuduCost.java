package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

public class KuduCost extends VolcanoCost {

  public static final RelOptCostFactory FACTORY = new Factory();

  static final VolcanoCost NEGATIVE_INFINITY =
    new VolcanoCost(
      Double.NEGATIVE_INFINITY,
      Double.NEGATIVE_INFINITY,
      Double.NEGATIVE_INFINITY) {
      public String toString() {
        return "{-inf}";
      }
    };

  KuduCost(double rowCount, double cpu, double io) {
    super(rowCount, cpu, io);
  }

  private static class Factory implements RelOptCostFactory {
    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
      return new VolcanoCost(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost() {
      return VolcanoCost.HUGE;
    }

    public RelOptCost makeInfiniteCost() {
      return VolcanoCost.INFINITY;
    }

    public RelOptCost makeTinyCost() {
      return VolcanoCost.TINY;
    }

    // We allow zero cost relations (see KuduProjectRel) so return -inf cost so that negative
    // costs are allowed (see
    public RelOptCost makeZeroCost() {
      return NEGATIVE_INFINITY;
    }
  }

}