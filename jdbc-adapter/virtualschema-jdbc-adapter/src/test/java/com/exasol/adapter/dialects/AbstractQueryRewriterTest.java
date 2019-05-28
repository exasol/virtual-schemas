package com.exasol.adapter.dialects;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.Map;

import com.exasol.*;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.sql.SqlStatement;

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