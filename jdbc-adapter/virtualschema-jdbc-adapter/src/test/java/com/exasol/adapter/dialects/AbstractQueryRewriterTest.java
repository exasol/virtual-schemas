package com.exasol.adapter.dialects;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Map;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.sql.SqlLiteralExactnumeric;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatement;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;

public abstract class AbstractQueryRewriterTest {
    protected static final String CONNECTION_NAME = "the_connection";
    private static final String CONNECTION_USER = "connection_user";
    private static final String CONNECTION_PW = "connection_secret";
    private static final String CONNECTION_ADDRESS = "connection_address";
    protected ExaConnectionInformation exaConnectionInformation;
    protected ExaMetadata exaMetadata;
    protected Map<String, String> rawProperties;
    protected SqlStatement statement;

    protected void mockExasolNamedConnection() throws ExaConnectionAccessException {
        when(this.exaMetadata.getConnection(any())).thenReturn(this.exaConnectionInformation);
        when(this.exaConnectionInformation.getUser()).thenReturn(CONNECTION_USER);
        when(this.exaConnectionInformation.getPassword()).thenReturn(CONNECTION_PW);
        when(this.exaConnectionInformation.getAddress()).thenReturn(CONNECTION_ADDRESS);
    }

    protected void setConnectionNameProperty() {
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, CONNECTION_NAME);
    }

    protected void setUserPasswordAndConnectionString() {
        this.rawProperties.put(AdapterProperties.USERNAME_PROPERTY, CONNECTION_USER);
        this.rawProperties.put(AdapterProperties.PASSWORD_PROPERTY, CONNECTION_PW);
        this.rawProperties.put(AdapterProperties.CONNECTION_STRING_PROPERTY, CONNECTION_ADDRESS);
    }

    protected SqlStatement createSimpleSelectStatement() {
        final ColumnMetadata columnMetadata = ColumnMetadata.builder().name("the_column")
                .type(DataType.createDecimal(18, 0)).build();
        final TableMetadata tableMetadata = new TableMetadata("DUAL", "", Arrays.asList(columnMetadata), "");
        final SqlNode fromClause = new SqlTable("DUAL", tableMetadata);
        final SqlSelectList selectList = SqlSelectList
                .createRegularSelectList(Arrays.asList(new SqlLiteralExactnumeric(BigDecimal.ONE)));
        return new SqlStatementSelect(fromClause, selectList, null, null, null, null, null);
    }

    protected Connection mockConnection() throws SQLException {
        final ResultSetMetaData metadataMock = mock(ResultSetMetaData.class);
        when(metadataMock.getColumnCount()).thenReturn(1);
        when(metadataMock.getColumnType(1)).thenReturn(Types.INTEGER);
        final PreparedStatement statementMock = mock(PreparedStatement.class);
        when(statementMock.getMetaData()).thenReturn(metadataMock);
        final Connection connectionMock = mock(Connection.class);
        when(connectionMock.prepareStatement(any())).thenReturn(statementMock);
        return connectionMock;
    }
}
