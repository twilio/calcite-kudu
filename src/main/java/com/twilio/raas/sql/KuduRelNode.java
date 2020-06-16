package com.twilio.raas.sql;

import com.google.common.collect.ImmutableList;
import com.twilio.raas.sql.rules.KuduToEnumerableConverter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.rex.RexLiteral;
import org.apache.kudu.client.KuduTable;

/**
 * A relational expression that represents a Kudu convention
 * This will be used by the {@link KuduToEnumerableConverter} to
 * translate into a {@link Blocks}. The {@code Blocks} will be used
 * to generate Byte code that represents the query and executes it
 * against the table.
 *
 * Each optimizer rule implemented in this module converts a boring
 * basic {@link RelNode} into a {@link KuduRelNode}.
 */
public interface KuduRelNode extends RelNode {

    Convention CONVENTION = new Convention.Impl("KUDU", KuduRelNode.class);

    /**
     * Each {@link KuduRelNode} implementation will accept the
     * {@link Implementor} and manipulate it. The {@code Implementor}
     * will then be used by the {@link KuduToEnumerableConverter} to
     * create a {@link Blocks} that will be used to generate Byte code
     * for the actual query.
     */
    void implement(Implementor implementor);

    /**
     * Implementor is a container to hold information required to execute a query or update to
     * kudu. Each {@link KuduRelNode} implementation will add information into this container.
     */
    class Implementor {
        public KuduTable kuduTable;
        public RelOptTable table;

        // information required for executing a query
        public final List<Integer> kuduProjectedColumns  = new ArrayList<>();
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
