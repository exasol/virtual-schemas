package com.exasol.adapter.jdbc;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.exasol.adapter.AdapterProperties;

class AbstractRemoteMetadataReaderTest {
    @Test
    void testGetSchemaAdapterNotesWithSqlException() throws SQLException {
        final Connection connectionMock = mockConnectionThrowingExceptionOnGetMetadata();
        final RemoteMetadataReader reader = new DummyRemoteMetadataReader(connectionMock,
                AdapterProperties.emptyProperties());
        assertThrows(RemoteMetadataReaderException.class, () -> reader.getSchemaAdapterNotes());
    }

    private Connection mockConnectionThrowingExceptionOnGetMetadata() throws SQLException {
        final Connection connectionMock = Mockito.mock(Connection.class);
        Mockito.when(connectionMock.getMetaData()).thenThrow(new SQLException("FAKE SQL exception"));
        return connectionMock;
    }

    @Test
    void testReadRemoteSchemaMetadataWithSqlException() throws SQLException {
        final Connection connectionMock = mockConnectionThrowingExceptionOnGetMetadata();
        final RemoteMetadataReader reader = new DummyRemoteMetadataReader(connectionMock,
                AdapterProperties.emptyProperties());
        assertThrows(RemoteMetadataReaderException.class, () -> reader.readRemoteSchemaMetadata());
    }

    @Test
    void testReadRemoteSchemaMetadataWithTableListAndSqlException() throws SQLException {
        final Connection connectionMock = mockConnectionThrowingExceptionOnGetMetadata();
        final RemoteMetadataReader reader = new DummyRemoteMetadataReader(connectionMock,
                AdapterProperties.emptyProperties());
        assertThrows(RemoteMetadataReaderException.class,
                () -> reader.readRemoteSchemaMetadata(Collections.emptyList()));
    }
}