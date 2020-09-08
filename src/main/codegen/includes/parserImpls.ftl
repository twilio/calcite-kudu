<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->


/**
 * Parses statement
 *   CREATE TABLE
 */
SqlNode SqlCreateTable() :
{
    SqlParserPos pos;
    SqlIdentifier tableName;
    boolean ifNotExists = false;
    SqlNodeList columnDefs;
    SqlNodeList pkConstraintColumnDefs = SqlNodeList.EMPTY;
    SqlNodeList tableOptions = SqlNodeList.EMPTY;
    SqlNodeList hashPartitionColumns;
    Integer hashBuckets;
    Integer numReplicas = -1;
}
{
    <CREATE>  { pos = getPos(); }
    <TABLE>
    [
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
    tableName = DualIdentifier()
    <LPAREN>
    columnDefs = ColumnDefList()
    [
    	<COMMA> <PRIMARY> <KEY>
        <LPAREN> pkConstraintColumnDefs = PkConstraintColumnDefList() <RPAREN>
    ]
    <RPAREN>
    <PARTITION> <BY>
    (
        <HASH> <LPAREN> hashPartitionColumns = ColumnNameList() <RPAREN> <PARTITIONS> hashBuckets = UnsignedIntLiteral()
    )
    [
        <NUM_REPLICAS> numReplicas = UnsignedIntLiteral()
    ]
    [
        <TBLPROPERTIES> <LPAREN> tableOptions = OptionList() <RPAREN>
    ]
    {
        return new SqlCreateTable(pos.plus(getPos()), tableName, ifNotExists, columnDefs,
        pkConstraintColumnDefs, tableOptions, hashPartitionColumns, hashBuckets, numReplicas);
    }
}

SqlColumnDefNode ColumnDef() :
{
    SqlIdentifier columnName;
    SqlDataTypeNode dataType;
    boolean isNull = true;
    boolean isPk = false;
    boolean isRowTimestamp = false;
    SortOrder sortOrder = SortOrder.ASC;
    SqlNode defaultValue = null;
    SqlNode encoding = null;
    SqlNode compression = null;
    Integer blockSize = -1;
    SqlNode comment = null;
    SqlParserPos pos;
}
{
    columnName = DualIdentifier()
    dataType = KuduDataType()
    [
        <PRIMARY> <KEY>
        {isPk = true;}
    ]
    [
        <ASC>
        {sortOrder = SortOrder.ASC;}
        |
        <DESC>
        {sortOrder = SortOrder.DESC;}
    ]
    [
        <NOT> <NULL>
        {isNull = false;}
        |
        <NULL>
        {isNull = true;}
    ]
    [
        <COLUMN_ENCODING>
        encoding = StringLiteral()
    ]
    [
        <COMPRESSION>
        compression = StringLiteral()
    ]
    [
        <DEFAULT_>
        defaultValue = Expression(ExprContext.ACCEPT_NONQUERY)
    ]
    [
        <BLOCK_SIZE>
        blockSize = UnsignedIntLiteral()
    ]
    [
        <ROW_TIMESTAMP>
        {isRowTimestamp = true;}
    ]
    [
        <COMMENT>
        comment = StringLiteral()
    ]
    {
        pos = columnName.getParserPosition().plus(getPos());
        return new SqlColumnDefNode(pos, columnName, dataType, isNull, isPk, isRowTimestamp,
        sortOrder, defaultValue, encoding, compression, blockSize, comment);
    }
}

SqlIdentifier DualIdentifier() :
{
    List<String> list = new ArrayList<String>();
    List<SqlParserPos> posList = new ArrayList<SqlParserPos>();
    String p;
}
{
    p = Identifier()
    {
        posList.add(getPos());
        list.add(p);
    }
    [
        <DOT>
            p = Identifier() {
                list.add(p);
                posList.add(getPos());
            }
    ]
    {
        SqlParserPos pos = SqlParserPos.sum(posList);
        return new SqlIdentifier(list, null, pos, posList);
    }
}

SqlNodeList ColumnDefList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> columnDefList;
}
{
    { pos = getPos(); }
    e = ColumnDef() { columnDefList = startList(e); }
    (
        <COMMA> e = ColumnDef() { columnDefList.add(e); }
    ) *
    {
        return new SqlNodeList(columnDefList, pos.plus(getPos()));
    }
}

SqlNodeList PkConstraintColumnDefList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> pkConstraintColumnDefList;
}
{
    { pos = getPos(); }
    e = ColumnDefInPkConstraint() { pkConstraintColumnDefList = startList(e); }
    (
        <COMMA> e = ColumnDefInPkConstraint() { pkConstraintColumnDefList.add(e); }
    ) *
    {
        return new SqlNodeList(pkConstraintColumnDefList, pos.plus(getPos()));
    }
}

SqlNodeList ColumnNameList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> columnNameList;
}
{
    { pos = getPos(); }
    e = ColumnName() { columnNameList = startList(e); }
    (
        <COMMA> e = ColumnName() { columnNameList.add(e); }
    ) *
    {
        return new SqlNodeList(columnNameList, pos.plus(getPos()));
    }
}

SqlNodeList OptionList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> optionList;
}
{
    { pos = getPos(); }
    e = Option() { optionList = startList(e); }
    (
        <COMMA> e = Option() { optionList.add(e); }
    ) *
    {
        return new SqlNodeList(optionList, pos.plus(getPos()));
    }
}

SqlDataTypeNode KuduDataType() :
{
    SqlTypeNameSpec typeName;
    SqlParserPos pos;
}
{
    typeName = TypeName()
    {
        pos = typeName.getParserPos().plus(getPos());
        return new SqlDataTypeNode(pos, typeName);
    }
}

SqlColumnDefInPkConstraintNode ColumnDefInPkConstraint() :
{
    SqlIdentifier columnName;
    SortOrder sortOrder = SortOrder.ASC;
    SqlParserPos pos;
}
{
    columnName = DualIdentifier()
    [
        <ASC>
        {sortOrder = SortOrder.ASC;}
        |
        <DESC>
        {sortOrder = SortOrder.DESC;}
    ]
    {
        pos = columnName.getParserPosition().plus(getPos());
        return new SqlColumnDefInPkConstraintNode(pos, columnName, sortOrder);
    }
}

SqlColumnNameNode ColumnName() :
{
    SqlIdentifier columnName;
    SqlParserPos pos;
}
{
    columnName = DualIdentifier()
    {
        pos = columnName.getParserPosition().plus(getPos());
        return new SqlColumnNameNode(pos, columnName);
    }
}

SqlOptionNode Option() :
{
    SqlNode key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = StringLiteral()
    <EQ>
    (
        value = Literal()
        {
            pos = key.getParserPosition().plus(getPos());
            return new SqlOptionNode(pos, key, (SqlLiteral) value);
        }
        |
        value = SimpleIdentifier()
        {
            pos = key.getParserPosition().plus(getPos());
            return new SqlOptionNode(pos, key, (SqlIdentifier)value);
        }
    )
}