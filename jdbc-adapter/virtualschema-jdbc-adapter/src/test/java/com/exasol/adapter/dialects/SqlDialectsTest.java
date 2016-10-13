package com.exasol.adapter.dialects;

import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import com.exasol.adapter.dialects.impl.ImpalaSqlDialect;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SqlDialectsTest {

    @Test
    public void testGetDialectByName() {
        SqlDialects dialects = new SqlDialects(ImmutableList.of(ExasolSqlDialect.NAME, ImpalaSqlDialect.NAME));
        SqlDialectContext context  = new SqlDialectContext(new SchemaAdapterNotes(".", "\"", false, false, false, false, false, false, false, false, false, false, true, false));
        assertTrue(dialects.getDialectByName("IMPALA", context).getClass().equals(ImpalaSqlDialect.class));
        
        assertTrue(dialects.getDialectByName("iMpAlA", context).getClass().equals(ImpalaSqlDialect.class));

        assertTrue(dialects.getDialectByName("impala", context).getClass().equals(ImpalaSqlDialect.class));
        
        assertTrue(dialects.getDialectByName("EXASOL", context).getClass().equals(ExasolSqlDialect.class));
        
        assertTrue(dialects.getDialectByName("unknown-dialect", context) == null);
    }
    
}
