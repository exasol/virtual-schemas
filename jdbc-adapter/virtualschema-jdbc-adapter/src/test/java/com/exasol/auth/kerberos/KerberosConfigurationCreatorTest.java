package com.exasol.auth.kerberos;

import static com.exasol.auth.kerberos.KerberosConfigurationCreator.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KerberosConfigurationCreatorTest {
    private static final String KEY_TAB_NAME = "ktbname";
    private static final String KERBEROS_CONFIG_NAME = "kbcname";
    private static final String JAAS_CONFIG_PATTERN = ".*/jaas_.*\\.conf";
    private static final String KERBEROS_CONFIG_PATTERN = ".*/krb_.*\\.conf";
    private static final String USER = "kerberos_user";
    private static final String PW = "ExaAuthType=Kerberos;" + KEY_TAB_NAME + ";" + KERBEROS_CONFIG_NAME;
    private KerberosConfigurationCreator creator;

    @BeforeEach
    void beforeEach() {
        this.creator = new KerberosConfigurationCreator();
    }

    @Test
    void testIsKerberosAuthenticationTrue() {
        assertThat(KerberosConfigurationCreator.isKerberosAuthentication(PW), equalTo(true));
    }

    @Test
    void testIsKerberosAuthenticationFalse() {
        assertThat(KerberosConfigurationCreator.isKerberosAuthentication("not a kerberose password"), equalTo(false));
    }

    @Test
    void testWriteKerberosConfigurationFiles() {
        this.creator.writeKerberosConfigurationFiles(USER, PW);
        assertAll( //
                () -> assertJaasConfigurationPathProperty(), //
                () -> assertKerberosConfigurationPathProperty(), //
                () -> assertUseSubjectCredentialsProperty(), //
                () -> assertJaasConfigurationFileContent(getJaasConfigPathFromProperty()), //
                () -> assertKerberosFileContent(), //
                () -> assertKeyTableFileContent(getJaasConfigPathFromProperty()));
    }

    private String getJaasConfigPathFromProperty() {
        return System.getProperty(LOGIN_CONFIG_PROPERTY);
    }

    private void assertJaasConfigurationPathProperty() {
        assertThat("JAAS configuration path", getJaasConfigPathFromProperty(), matchesPattern(JAAS_CONFIG_PATTERN));
    }

    private void assertKerberosConfigurationPathProperty() {
        assertThat("Kerberos configuration path", getKerberosConfigFromProperty(), //
                matchesPattern(KERBEROS_CONFIG_PATTERN));
    }

    private String getKerberosConfigFromProperty() {
        return System.getProperty(KERBEROS_CONFIG_PROPERTY);
    }

    private void assertUseSubjectCredentialsProperty() {
        assertThat("Use subject credentials", System.getProperty(USE_SUBJECT_CREDENTIALS_ONLY_PROPERTY),
                equalTo("false"));
    }

    private void assertJaasConfigurationFileContent(final String jaasConfigurationPath) throws IOException {
        final String content = getJaasConfigContent(jaasConfigurationPath);
        assertAll(() -> assertThat(content, startsWith("Client {")), //
                () -> assertThat(content, containsString("principal=\"" + USER + "\"")));
    }

    private String getJaasConfigContent(final String jaasConfigurationPath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(jaasConfigurationPath)));
    }

    private void assertKerberosFileContent() {
        assertThat(new File(getKerberosConfigFromProperty()), anExistingFile());
    }

    private void assertKeyTableFileContent(final String jaasConfigurationPath) throws IOException {
        final String jaasConfigContent = getJaasConfigContent(jaasConfigurationPath);
        String keyTabPath = jaasConfigContent.substring(jaasConfigContent.indexOf("keyTab=\"") + 8);
        keyTabPath = keyTabPath.substring(0, keyTabPath.indexOf("\""));
        assertThat("Key tab file: " + keyTabPath, new File(keyTabPath), anExistingFile());
    }

    @ValueSource(strings = { "", "missing preamble;foo;bar", "ExaAuthType=Kerberos;missing next part",
            "ExaAuthType=Kerberos;too;many;parts" })
    @ParameterizedTest
    void testIllegalKerberosPasswordThrowsException(final String password) {
        assertThrows(KerberosConfigurationCreatorException.class,
                () -> this.creator.writeKerberosConfigurationFiles("anyone", password));
    }
}