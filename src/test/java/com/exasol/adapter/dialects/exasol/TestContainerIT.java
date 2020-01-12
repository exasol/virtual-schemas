package com.exasol.adapter.dialects.exasol;

import java.sql.*;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.bucketfs.Bucket;
import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;

@Tag("integration")
@Testcontainers
public class TestContainerIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestContainerIT.class);

    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> container = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE) //
                    .withLogConsumer(new Slf4jLogConsumer(LOGGER));
    private static Statement statement;

    @BeforeAll
    static void beforeAll() throws SQLException {
        final Bucket bucket = container.getDefaultBucket();
        final Connection connection = container.createConnectionForUser(container.getUsername(),
                container.getPassword());
        statement = connection.createStatement();
        createTestSchema();

    }

    private static void createTestSchema() throws SQLException {
        statement.execute("CREATE SCHEMA TEST");
    }

    @Test
    void testDataTypeMapping() throws InterruptedException {
        TimeUnit.SECONDS.sleep(20);
        LOGGER.info("Reached the test");
    }
}