package com.exasol.adapter.dialects.exasol;

import com.exasol.bucketfs.Bucket;
import com.exasol.bucketfs.BucketAccessException;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Path;
import java.sql.*;
import java.util.concurrent.TimeUnit;

@Tag("integration")
@Testcontainers
public class TestTravisIT {
    private static final String ROW_LEVEL_SECURITY_JAR_NAME_AND_VERSION = "row-level-security-dist-0.2.0.jar";
    private static final String VIRTUAL_SCHEMA_RLS_JDBC_NAME = "VIRTUAL_SCHEMA_RLS_JDBC";
    private static final String VIRTUAL_SCHEMA_RLS_JDBC_LOCAL_NAME = "VIRTUAL_SCHEMA_RLS_JDBC_LOCAL";
    private static final String VIRTUAL_SCHEMA_RLS_EXA_NAME = "VIRTUAL_SCHEMA_RLS_EXA";
    private static final String VIRTUAL_SCHEMA_RLS_EXA_LOCAL_NAME = "VIRTUAL_SCHEMA_RLS_EXA_LOCAL";
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> container = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE);
    public static final String RLS_SALES_UNPROTECTED = "RLS_SALES_UNPROTECTED";
    public static final String RLS_SALES_TENANTS = "RLS_SALES_TENANTS";
    public static final String RLS_SALES_ROLES = "RLS_SALES_ROLES";
    private static Statement statement;

    @BeforeAll
    static void beforeAll() throws SQLException, BucketAccessException, InterruptedException {
        final Bucket bucket = container.getDefaultBucket();
        final Connection connection = container.createConnectionForUser(container.getUsername(),
                container.getPassword());
        statement = connection.createStatement();
    }

    @Test
    void testExaRlsUsersTableIsFilteredOut() throws InterruptedException {
        TimeUnit.SECONDS.sleep(20);
        System.out.println("Reaches test.");
    }
}
