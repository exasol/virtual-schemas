package com.exasol.adapter.dialects;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import com.exasol.adapter.dialects.impl.MysqlSqlDialect;
import com.exasol.adapter.dialects.impl.OracleSqlDialect;

public class SqlDialectLoaderTest {
    @Test
    public void testFindDialectDriverClassNames() {
        assertThat(SqlDialectLoader.getInstance().findDriverClassNames(),
                hasItems("ExasolSqlDialect", "MysqlSqlDialect", "OracleSqlDialect"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFindDialectDriverClasss() {
        assertThat(SqlDialectLoader.getInstance().findDriverClasses(),
                hasItems(ExasolSqlDialect.class, MysqlSqlDialect.class, OracleSqlDialect.class));
    }
}