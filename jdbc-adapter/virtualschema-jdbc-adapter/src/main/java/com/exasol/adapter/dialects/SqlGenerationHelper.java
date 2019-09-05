package com.exasol.adapter.dialects;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

/**
 * Contains functions that are helpful during SQL generation.
 * <p>
 * These functions are used by the adapters during SQL generation.
 */
public final class SqlGenerationHelper {
    private SqlGenerationHelper() {
        //intentionally left blank
    }

    /**
     * Check if selected node requires cast or not.
     *
     * @param selectList SQL select list
     * @param nodeRequiresCast node that requires cast
     * @return true if selected node requires cast
     */
    public static boolean selectListRequiresCasts(final SqlSelectList selectList,
          final Predicate<SqlNode> nodeRequiresCast) {
        boolean requiresCasts = false;
        final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        final int columnId = 0;
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        addMetadata(select.getFromClause(), tableMetadata);
        for (final TableMetadata tableMeta : tableMetadata) {
            for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                if (nodeRequiresCast.test(new SqlColumn(columnId, columnMeta))) {
                    requiresCasts = true;
                }
            }
        }
        return requiresCasts;
    }

    /**
     * Add metadata to the list.
     *
     * @param node SQL node to get metadata from
     * @param metadata list of TableMetadata
     */
    public static void addMetadata(final SqlNode node, final List<TableMetadata> metadata) {
        if (node.getType() == SqlNodeType.TABLE) {
            final SqlTable table = (SqlTable) node;
            metadata.add(table.getMetadata());
        } else if (node.getType() == SqlNodeType.JOIN) {
            final SqlJoin join = (SqlJoin) node;
            addMetadata(join.getLeft(), metadata);
            addMetadata(join.getRight(), metadata);
        }
    }
}