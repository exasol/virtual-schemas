package com.exasol.adapter.dialects;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Contains functions that are helpful during SQL generation.
 * <p>
 * These functions are used by the adapters during SQL generation.
 */
public final class SqlGenerationHelper {
    private SqlGenerationHelper() {
        //intentionally left blank
    }

    public static boolean selectListRequiresCasts(final SqlSelectList selectList,
          final Predicate<SqlNode> nodeRequiresCast) {
        boolean requiresCasts = false;
        final SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        final int columnId = 0;
        final List<TableMetadata> tableMetadata = new ArrayList<>();
        getMetadataFrom(select.getFromClause(), tableMetadata);
        for (final TableMetadata tableMeta : tableMetadata) {
            for (final ColumnMetadata columnMeta : tableMeta.getColumns()) {
                if (nodeRequiresCast.test(new SqlColumn(columnId, columnMeta))) {
                    requiresCasts = true;
                }
            }
        }
        return requiresCasts;
    }

    public static void getMetadataFrom(final SqlNode node, final List<TableMetadata> metadata) {
        if (node.getType() == SqlNodeType.TABLE) {
            final SqlTable table = (SqlTable) node;
            metadata.add(table.getMetadata());
        } else if (node.getType() == SqlNodeType.JOIN) {
            final SqlJoin join = (SqlJoin) node;
            getMetadataFrom(join.getLeft(), metadata);
            getMetadataFrom(join.getRight(), metadata);
        }
    }
}