package com.exasol.adapter.dialects;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlColumn;
import com.exasol.adapter.sql.SqlJoin;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlNodeType;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;

/**
 * Contains functions that are helpful during SQL generation.
 *
 * These functions are used by the adapters during SQL generation.
 */
public class SqlGenerationHelper {

    public boolean selectListRequiresCasts(SqlSelectList selectList, Predicate<SqlNode> nodeRequiresCast) throws AdapterException {
        boolean requiresCasts = false;

        // Do as if the user has all columns in select list
        SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        int columnId = 0;
        List<TableMetadata> tableMetadata = new ArrayList<TableMetadata>();
        getMetadataFrom(select.getFromClause(), tableMetadata );
        for (TableMetadata tableMeta : tableMetadata) {
            for (ColumnMetadata columnMeta : tableMeta.getColumns()) {
                    if (nodeRequiresCast.test(new SqlColumn(columnId, columnMeta))) {
                    requiresCasts = true;
                    }
            }
        }

        return requiresCasts;
    }

    public void getMetadataFrom(SqlNode node, List<TableMetadata> metadata){
        if (node.getType() == SqlNodeType.TABLE) {
            SqlTable table = (SqlTable) node;
            metadata.add(table.getMetadata());
        } else if (node.getType() == SqlNodeType.JOIN) {
            SqlJoin join = (SqlJoin) node;
            getMetadataFrom(join.getLeft(), metadata);
            getMetadataFrom(join.getRight(), metadata);
        }
    }
}