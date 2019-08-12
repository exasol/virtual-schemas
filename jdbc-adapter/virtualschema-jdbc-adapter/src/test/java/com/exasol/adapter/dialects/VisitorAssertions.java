package com.exasol.adapter.dialects;

import com.exasol.adapter.*;
import com.exasol.adapter.sql.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class VisitorAssertions {
    public static void assertSqlNodeConvertedToOne(final SqlSelectList sqlSelectList, final SqlNodeVisitor visitor)
            throws AdapterException {
        assertThat(visitor.visit(sqlSelectList), equalTo("1"));
    }

    public static void assertSqlNodeConvertedToAsterisk(final SqlSelectList selectList, final SqlNodeVisitor visitor)
            throws AdapterException {
        assertThat(visitor.visit(selectList), equalTo("*"));
    }
}
