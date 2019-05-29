package com.exasol.auth.kerberos;

import static com.exasol.adapter.jdbc.KerberosUtils.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KerberosFileCreatorTest {
    private KerberosFileCreator creator;

    @BeforeEach
    void beforeEach() {
        this.creator = new KerberosFileCreator();
    }

    @Test
    void testWriteKerberosConfigurationFiles() {
        this.creator.writeKerberosConfigurationFiles();
        final String jaasConfigurationPath = System.getProperty(LOGIN_CONFIG_PROPERTY);
        final String kerberosConfigurationPath = System.getProperty(KERBEROS_CONFIGURATION_PROPERTY);
        final String useSubjectCredentialsOnly = System.getProperty(USE_SUBJECT_CREDENTIALS_ONLY_PROPERTY);
        assertAll(
                () -> assertThat("JAAS configuration path", jaasConfigurationPath, matchesPattern(".*/krb_.*\\.conf")),
                () -> assertThat("Kerberos configuration path", kerberosConfigurationPath,
                        matchesPattern(".*/kt_.*\\.keytab")),
                () -> assertThat("Use subject credentials", useSubjectCredentialsOnly, equalTo("false")),
                () -> assertJaasConfigurationFileContent(), () -> assertKerberoseFileContent(),
                () -> assertKeyTableFileContent());
    }

    private void assertJaasConfigurationFileContent() {
        // TODO Auto-generated method stub
    }

    private void assertKerberoseFileContent() {
        // TODO Auto-generated method stub
    }

    private void assertKeyTableFileContent() {
        // TODO Auto-generated method stub
    }
}