package com.twilio.raas.sql;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.AbstractRelNode;
import java.util.ArrayList;
import java.util.List;
import org.apache.kudu.client.KuduPredicate;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.kudu.client.KuduTable;

/**
 * A relational expression that represents a Kudu convention
 * This will be used by the {@link KuduToEnumerableConverter} to
 * translate into a {@link Blocks}. The {@code Blocks} will be used
 * to generate Byte code that represents the query and executes it
 * against the table.
 *
 * Each optimizer rule implemented in this module converts a boring
 * basic {@link RelNode} into a {@link KuduRel}. 
 */
public interface KuduRel extends RelNode {
  
    Convention CONVENTION = new Convention.Impl("KUDU", KuduRel.class);

    /**
     * Each {@link KuduRel} implementation will accept the
     * {@link Implementor} and manipulate it. The {@code Implementor}
     * will then be used by the {@link KuduToEnumerableConverter} to
     * create a {@link Blocks} that will be used to generate Byte code
     * for the actual query.
     */
    void implement(Implementor implementor);

    /**
     * Implementor is a container that represents the set of 
     * Kudu Scans. Each {@link KuduRel} implementation will 
     * add information into this container.
     */
    class Implementor {
        public final List<List<CalciteKuduPredicate>> predicates = new ArrayList<>();
        public final List<Integer> kuduProjectedColumns  = new ArrayList<>();
        public KuduTable openedTable;
        public RelOptTable table;
        public int limit = -1;

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            ((KuduRel) input).implement(this);
        }
    }
}
				 
