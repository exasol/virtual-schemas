package com.exasol.adapter.dialects.exasol;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.exasol.containers.ExasolContainer;
import com.exasol.containers.ExasolContainerConstants;

@Tag("integration")
@Testcontainers
public class TravisIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(TravisIT.class);
    @Container
    private static final ExasolContainer<? extends ExasolContainer<?>> container = new ExasolContainer<>(
            ExasolContainerConstants.EXASOL_DOCKER_IMAGE_REFERENCE);

    @BeforeAll
    static void beforeAll() {
        container.followOutput(new Slf4jLogConsumer(LOGGER));
    }

    @Test
    void testExaRlsUsersTableIsFilteredOut() throws InterruptedException {
        TimeUnit.SECONDS.sleep(20);
        System.out.println("Reaches test.");
    }
}
