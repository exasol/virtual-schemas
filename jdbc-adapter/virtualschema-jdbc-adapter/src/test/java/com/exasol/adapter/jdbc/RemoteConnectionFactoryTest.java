package com.exasol.adapter.jdbc;

import static com.exasol.auth.kerberos.KerberosConfigurationCreator.KERBEROS_CONFIG_PROPERTY;
import static com.exasol.auth.kerberos.KerberosConfigurationCreator.LOGIN_CONFIG_PROPERTY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.exasol.*;
import com.exasol.adapter.AdapterProperties;

@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
class RemoteConnectionFactoryTest {
    private static final String CONNECTION_NAME = "testConnection";
    private static final String DERBY_INSTANT_JDBC_CONNECTION_STRING = "jdbc:derby:memory:test;create=true;";
    private static final String USER = "testUserName";
    private Map<String, String> rawProperties;

    @Mock
    private ExaMetadata exaMetadataMock;
    @Mock
    private ExaConnectionInformation exaConnectionMock;

    @BeforeEach
    void beforeEach() {
        this.rawProperties = new HashMap<>();
    }

    @Test
    void testCreateConnectionWithConnectionName() throws ExaConnectionAccessException, SQLException {
        this.rawProperties.put("CONNECTION_NAME", CONNECTION_NAME);
        when(this.exaMetadataMock.getConnection(CONNECTION_NAME)).thenReturn(this.exaConnectionMock);
        when(this.exaConnectionMock.getUser()).thenReturn(USER);
        when(this.exaConnectionMock.getPassword()).thenReturn("pass");
        when(this.exaConnectionMock.getAddress()).thenReturn(DERBY_INSTANT_JDBC_CONNECTION_STRING);
        assertThat(createConnection().getMetaData().getUserName(), equalTo(USER));
    }

    private Connection createConnection() throws SQLException {
        final RemoteConnectionFactory factory = new RemoteConnectionFactory();
        final AdapterProperties properties = new AdapterProperties(this.rawProperties);
        return factory.createConnection(this.exaMetadataMock, properties);
    }

    @Test
    void testCreateConnectionWithConnectionDetailsInProperties() throws ExaConnectionAccessException, SQLException {
        this.rawProperties.put("CONNECTION_STRING", DERBY_INSTANT_JDBC_CONNECTION_STRING);
        this.rawProperties.put("USERNAME", USER);
        this.rawProperties.put("PASSWORD", "testPassword");
        assertThat(createConnection().getMetaData().getUserName(), equalTo(USER));
    }

    @Test
    void testCreateConnectionWithConnectionDetailsInPropertiesAndEmptyConnectionName()
            throws ExaConnectionAccessException, SQLException {
        this.rawProperties.put("CONNECTION_NAME", "");
        this.rawProperties.put("CONNECTION_STRING", DERBY_INSTANT_JDBC_CONNECTION_STRING);
        this.rawProperties.put("USERNAME", USER);
        this.rawProperties.put("PASSWORD", "testPassword");
        assertThat(createConnection().getMetaData().getUserName(), equalTo(USER));
    }

    @Test
    void testCreateConnectionWithUnaccessibleNamedConnectionThrowsException()
            throws ExaConnectionAccessException, SQLException {
        when(this.exaMetadataMock.getConnection(CONNECTION_NAME))
                .thenThrow(new ExaConnectionAccessException("FAKE connection access exception"));
        this.rawProperties.put("CONNECTION_NAME", CONNECTION_NAME);
        assertThrows(RemoteConnectionException.class, () -> createConnection());
    }

    @Test
    void testCreateConnectionWithKerberosDetailsInNamedConnection() throws SQLException, ExaConnectionAccessException {
        final String principal = "the_kerberos_principal";
        final String base64EncodedKerberosConfig = DatatypeConverter.printBase64Binary("<a></a>".getBytes());
        final String base64EncodedKeyTab = DatatypeConverter.printBase64Binary("<b></b>".getBytes());
        final String credentialString = "ExaAuthType=Kerberos;" + base64EncodedKerberosConfig + ";"
                + base64EncodedKeyTab;
        when(this.exaConnectionMock.getUser()).thenReturn(principal);
        when(this.exaConnectionMock.getPassword()).thenReturn(credentialString);
        when(this.exaConnectionMock.getAddress()).thenReturn(DERBY_INSTANT_JDBC_CONNECTION_STRING);
        when(this.exaMetadataMock.getConnection(CONNECTION_NAME)).thenReturn(this.exaConnectionMock);
        this.rawProperties.put("CONNECTION_NAME", CONNECTION_NAME);
        createConnection(); // return value ignored on purpose
        assertAll(() -> assertThat(System.getProperty(KERBEROS_CONFIG_PROPERTY), matchesPattern(".*/krb_.*\\.conf")),
                () -> assertThat(System.getProperty(LOGIN_CONFIG_PROPERTY), matchesPattern(".*/jaas_.*\\.conf")));
    }
}