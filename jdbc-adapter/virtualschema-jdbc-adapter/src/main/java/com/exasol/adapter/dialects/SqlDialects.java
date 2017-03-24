package com.exasol.adapter.dialects;

import com.exasol.adapter.dialects.impl.*;

import java.util.List;

/**
 * Manages a set of supported SqlDialects.
 */
public class SqlDialects {

    private List<String> supportedDialects;

    private List<Class<? extends SqlDialect>> dialects;

    public SqlDialects(List<String> supportedDialects) {
        this.supportedDialects = supportedDialects;
    }

    public boolean isSupported(String dialectName) {
        for (String curName : supportedDialects) {
            if (curName.equalsIgnoreCase(dialectName)) {
                return true;
            }
        }
        return false;
    }

    public SqlDialect getDialectByName(String name, SqlDialectContext context) {
        if (name.equalsIgnoreCase(GenericSqlDialect.NAME)) {
            return new GenericSqlDialect(context);
        } else if (name.equalsIgnoreCase(ExasolSqlDialect.NAME)) {
            return new ExasolSqlDialect(context);
        } else if (name.equalsIgnoreCase(HiveSqlDialect.NAME)) {
            return new HiveSqlDialect(context);
        } else if (name.equalsIgnoreCase(ImpalaSqlDialect.NAME)) {
            return new ImpalaSqlDialect(context);
        } else if (name.equalsIgnoreCase(MysqlSqlDialect.NAME)) {
            return new MysqlSqlDialect(context);
        } else if (name.equalsIgnoreCase(OracleSqlDialect.NAME)) {
            return new OracleSqlDialect(context);
        } else if (name.equalsIgnoreCase(TeradataSqlDialect.NAME)) {
            return new TeradataSqlDialect(context);
        } else if (name.equalsIgnoreCase(RedshiftSqlDialect.NAME)) {
            return new RedshiftSqlDialect(context);
        } else if (name.equalsIgnoreCase(DB2SqlDialect.NAME)) {
	        return new DB2SqlDialect(context);
        } else if (name.equalsIgnoreCase(SqlServerSqlDialect.NAME)) {
        	return new SqlServerSqlDialect(context);
        } else if (name.equalsIgnoreCase(PostgreSQLSqlDialect.NAME)) {
        	return new PostgreSQLSqlDialect(context);
        }
        else {
            return null;
        }
    }

    public List<Class<? extends SqlDialect>> getDialects() {
        return dialects;
    }

    public String getDialectsString() {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String curName : supportedDialects) {
            if (!first) {
                builder.append(", ");
            }
            builder.append(curName);
            first = false;
        }
        return builder.toString();
    }
}
