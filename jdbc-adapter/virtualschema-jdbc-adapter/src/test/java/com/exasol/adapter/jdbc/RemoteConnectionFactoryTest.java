package com.exasol.adapter.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.exasol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.exasol.adapter.AdapterProperties;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class RemoteConnectionFactoryTest {
    private static final String USER = "testUserName";

    @Mock
    private ExaMetadata exaMetadataMock;
    @Mock
    private ExaConnectionInformation exaConnectionMock;

    @Test
    void testCreateConnectionWithConnectionName() throws ExaConnectionAccessException, SQLException {
        Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put("CONNECTION_NAME", "testConnection");
        RemoteConnectionFactory factory = new RemoteConnectionFactory();
        when(exaMetadataMock.getConnection("testConnection")).thenReturn(exaConnectionMock);
        when(exaConnectionMock.getUser()).thenReturn(USER);
        when(exaConnectionMock.getPassword()).thenReturn("pass");
        when(exaConnectionMock.getAddress()).thenReturn("jdbc:derby:memory:test;create=true;");
        Connection connection = factory.createConnection(exaMetadataMock, new AdapterProperties(rawProperties));
        assertAll(() -> assertThat(connection.getMetaData().getUserName(), equalTo(USER)));
    }

    @Test
    void testCreateConnection() throws ExaConnectionAccessException, SQLException {
        Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put("CONNECTION_STRING", "jdbc:derby:memory:test;create=true;");
        rawProperties.put("USERNAME", USER);
        rawProperties.put("PASSWORD", "testPassword");
        RemoteConnectionFactory factory = new RemoteConnectionFactory();
        Connection connection = factory.createConnection(exaMetadataMock, new AdapterProperties(rawProperties));
        assertAll(() -> assertThat(connection.getMetaData().getUserName(), equalTo(USER)));
    }
}
