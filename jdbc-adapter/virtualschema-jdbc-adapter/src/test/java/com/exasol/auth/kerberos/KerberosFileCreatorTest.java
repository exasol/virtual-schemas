package com.exasol.auth.kerberos;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KerberosFileCreatorTest {
    private KerberosFileCreator creator;

    @BeforeEach
    void beforeEach() {
        this.creator = new KerberosFileCreator();
    }

    @Test
    void testWriteKerberosConfigurationFile() {
        fail("Not yet implemented");
    }
}