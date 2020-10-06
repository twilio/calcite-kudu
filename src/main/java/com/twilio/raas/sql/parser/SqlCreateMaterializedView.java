package com.twilio.raas.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE MATERIALIZED VIEW} statement.
 */
public class SqlCreateMaterializedView extends SqlCall {
    public final SqlOperator operator;
    public final SqlIdentifier cubeName;
    public final boolean ifNotExists;
    public final SqlSelect query;

    /** Creates a SqlCreateView. */
    SqlCreateMaterializedView(SqlParserPos pos, SqlIdentifier cubeName,
                              boolean ifNotExists, SqlSelect query) {
        super(pos);
        this.operator = new SqlDdlOperator("CREATE MATERIALIZED VIEW", SqlKind.CREATE_MATERIALIZED_VIEW);
        this.cubeName = Objects.requireNonNull(cubeName);
        this.ifNotExists = ifNotExists;
        this.query = Objects.requireNonNull(query);
    }

    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(cubeName, query);
    }

    public SqlOperator getOperator() {
        return operator;
    }

    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        writer.keyword("MATERIALIZED VIEW");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        cubeName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, 0, 0);
    }
}
