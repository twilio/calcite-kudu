package com.twilio.raas.sql.rel;

import com.google.common.collect.Lists;
import com.twilio.raas.sql.KuduRelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class KuduProjectRel extends Project implements KuduRelNode {
    private List<Integer> projectedColumnsIndexes = Lists.newArrayList();

    public KuduProjectRel(RelOptCluster cluster, RelTraitSet traitSet,
                          RelNode input, List<? extends RexNode> projects,
                          RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
    }
    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
                        List<RexNode> projects, RelDataType rowType) {
        return new KuduProjectRel(getCluster(), traitSet, input, projects, rowType);
    }
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // The planner will set the cost to VolcanoCost.TINY if the cost is zero which will cause
        // this rel to not get chosen if the projection contains a function expression (since we
        // wrap this rel with a LogicalProject)
        // TODO find a better way of ensuring KuduProjectRel is chosen
        double dRows = Double.MIN_VALUE;
        double dCpu = 0;
        double dIo = 0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        final KuduColumnVisitor visitor = new KuduColumnVisitor();
        projectedColumnsIndexes
            .addAll(getNamedProjects()
                .stream()
                .map(expressionNamePair -> expressionNamePair.left.accept(visitor))
                .flatMap(Collection::stream)
                .collect(Collectors.toList()));
        // Since KuduProjectRel has a cost of zero, if we have a KuduProjectRel that wraps
        // another KuduProjectRel ProjectRemoveRule will not be able to combine them since the
        // the cost would not decrease
        // We always chose the columns that are projected by the outermost KuduProjectRel
        if (!implementor.kuduProjectedColumns.isEmpty()) {
            List<Integer> prevProjectedColumns =
                    Lists.newArrayList(implementor.kuduProjectedColumns);
            implementor.kuduProjectedColumns.clear();
            projectedColumnsIndexes.stream().forEach(
                    index -> {
                        implementor.kuduProjectedColumns.add(prevProjectedColumns.get(index));
                    });
        }
        else {
            implementor.kuduProjectedColumns.addAll(projectedColumnsIndexes);
        }
    }

    public static class KuduColumnVisitor extends RexVisitorImpl<List<Integer>> {

      public KuduColumnVisitor() {
            super(true);
        }

      @Override
      public List<Integer> visitInputRef(RexInputRef inputRef) {
          return Lists.newArrayList(inputRef.getIndex());
      }

      /**
       * Extact the columns used an inputs to functions
       * @param call function call
       * @return list of column indexes
       */
      @Override
      public List<Integer> visitCall(RexCall call) {
          List<Integer> columnIndexes = Lists.newArrayList();
          for (RexNode operand : call.operands) {
              List<Integer> operandColumnIndexes = operand.accept(this);
              if (operandColumnIndexes !=null) {
                  columnIndexes.addAll(operandColumnIndexes);
              }
          }
          return columnIndexes;
      }

      @Override
      public List<Integer> visitLocalRef(RexLocalRef localRef) {
        return Collections.emptyList();
      }

      @Override
      public List<Integer> visitLiteral(RexLiteral literal) {
        return Collections.emptyList();
      }

      @Override
      public List<Integer> visitCorrelVariable(RexCorrelVariable correlVariable) {
        return Collections.emptyList();
      }

      @Override
      public List<Integer> visitDynamicParam(RexDynamicParam dynamicParam) {
        return Collections.emptyList();
      }

      @Override
      public List<Integer> visitRangeRef(RexRangeRef rangeRef) {
        return Collections.emptyList();
      }

      @Override
      public List<Integer> visitTableInputRef(RexTableInputRef ref) {
        return Collections.emptyList();
      }

      @Override
      public List<Integer> visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        return Collections.emptyList();
      }
    }

    /**
     * Used to transform projections of a  {@link org.apache.calcite.rel.logical.LogicalProject}
     * to use the output of a {@link KuduProjectRel}
     */
    public static class KuduProjectTransformer extends RexVisitorImpl<RexNode> {

        // map from column index to type
        private final LinkedHashMap<Integer, RelDataTypeField> projectedColumnTpRedDataTypeFieldMap;

        public KuduProjectTransformer() {
            super(true);
            projectedColumnTpRedDataTypeFieldMap = new LinkedHashMap<>();
        }

        public LinkedHashMap<Integer, RelDataTypeField> getProjectedColumnToRelDataTypeFieldMap() {
            return projectedColumnTpRedDataTypeFieldMap;
        }

        private int getKuduProjectionColumnIndex(int colIndex) {
            Iterator<Integer> iter = projectedColumnTpRedDataTypeFieldMap.keySet().iterator();
            int kuduProjectionColumnIndex = 0;
            while (iter.hasNext()) {
                if (iter.next() == colIndex) {
                  return kuduProjectionColumnIndex;
                }
                ++kuduProjectionColumnIndex;
            }
            throw new IllegalArgumentException("Unable to find column index " + colIndex + " in " +
                    "for projected columns " + projectedColumnTpRedDataTypeFieldMap);
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            int colIndex = inputRef.getIndex();
            if (!projectedColumnTpRedDataTypeFieldMap.containsKey(colIndex)) {
                RelDataTypeField relDataTypeField = new RelDataTypeFieldImpl(inputRef.getName(),
                        colIndex, inputRef.getType());
                projectedColumnTpRedDataTypeFieldMap.put(colIndex, relDataTypeField);
            }
            // create a new RexInputRef that refers to the output of the KuduProjection
            return new RexInputRef(getKuduProjectionColumnIndex(colIndex), inputRef.getType());
        }

        /**
         * Extract the columns used an inputs to functions
         * @param call function call
         * @return list of column indexes
         */
        @Override
        public RexCall visitCall(RexCall call) {
            List<RexNode> transformedOperands =
                    Lists.newArrayListWithExpectedSize(call.getOperands().size());
            for (RexNode operand : call.operands) {
                transformedOperands.add(operand.accept(this));
            }
            return call.clone(call.getType(), transformedOperands);
        }

        @Override
        public RexLocalRef visitLocalRef(RexLocalRef localRef) {
            return localRef;
        }

        @Override
        public RexLiteral visitLiteral(RexLiteral literal) {
            return literal;
        }

        @Override
        public RexOver visitOver(RexOver over) {
            return over;
        }

        @Override
        public RexCorrelVariable visitCorrelVariable(RexCorrelVariable correlVariable) {
            return correlVariable;
        }

        @Override
        public RexDynamicParam visitDynamicParam(RexDynamicParam dynamicParam) {
            return dynamicParam;
        }

        @Override
        public RexRangeRef visitRangeRef(RexRangeRef rangeRef) {
            return rangeRef;
        }

        @Override
        public RexFieldAccess visitFieldAccess(RexFieldAccess fieldAccess) {
            return fieldAccess;
        }

        @Override
        public RexSubQuery visitSubQuery(RexSubQuery subQuery) {
            return subQuery;
        }

        @Override
        public RexTableInputRef visitTableInputRef(RexTableInputRef fieldRef){
           return fieldRef;
        }

        @Override
        public RexPatternFieldRef visitPatternFieldRef(RexPatternFieldRef fieldRef){
            return fieldRef;
        }

    }
}
