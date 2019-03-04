package com.exasol.adapter.jdbc;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.Charset;

/**
 * Utility class to establish JDBC connections with Kerberos authentication
 */
public class KerberosUtils {

    private static final String KRB_KEY = "ExaAuthType=Kerberos;";

    public static boolean isKerberosAuth(String pass) {
        if (pass == null) {
            return false;
        }
        return pass.indexOf(KRB_KEY) == 0;
    }

    public static void configKerberos(String user, String pass) throws Exception {
        try {
            pass = pass.replaceFirst(KRB_KEY, "");
        } catch (Exception e) {
            throw new RuntimeException("Could not find " + KRB_KEY + " in password: " + e.getMessage());
        }
        String[] confKeytab = pass.split(";");
        if (confKeytab.length != 2)
            throw new RuntimeException("Invalid Kerberos conf/keytab");
        File kerberosBaseDir = new File("/tmp");
        File krbDir = File.createTempFile("kerberos_", null, kerberosBaseDir);
        krbDir.delete();
        krbDir.mkdir();
        krbDir.deleteOnExit();
        String krbConfPath = writePath(krbDir, confKeytab[0], "krb_", ".conf");
        String keytabPath = writePath(krbDir, confKeytab[1], "kt_", ".keytab");
        String jaasConfigPath = writeJaasConfig(krbDir, user, keytabPath);
        System.setProperty("java.security.auth.login.config", jaasConfigPath);
        System.setProperty("java.security.krb5.conf", krbConfPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    }

    private static String writePath(File krbDir, String confKeyTab, String prefix, String suffix) throws Exception {
        File file = File.createTempFile(prefix, suffix, krbDir);
        file.deleteOnExit();
        try (FileOutputStream os = new FileOutputStream(file);) {
            os.write(DatatypeConverter.parseBase64Binary(confKeyTab));
        }
        return file.getCanonicalPath();
    }

    private static String writeJaasConfig(File krbDir, String princ, String keytabPath) throws Exception {
        File file = File.createTempFile("jaas_", ".conf", krbDir);
        file.deleteOnExit();
        String jaasData;
        jaasData = "Client {\n";
        jaasData += "com.sun.security.auth.module.Krb5LoginModule required\n";
        jaasData += "principal=\"" + princ + "\"\n";
        jaasData += "useKeyTab=true\n";
        jaasData += "keyTab=\"" + keytabPath + "\"\n";
        jaasData += "doNotPrompt=true\n";
        jaasData += "useTicketCache=false;\n";
        jaasData += "};\n";
        jaasData += "com.sun.security.jgss.initiate {\n";
        jaasData += "com.sun.security.auth.module.Krb5LoginModule required\n";
        jaasData += "principal=\"" + princ + "\"\n";
        jaasData += "useKeyTab=true\n";
        jaasData += "keyTab=\"" + keytabPath + "\"\n";
        jaasData += "doNotPrompt=true\n";
        jaasData += "useTicketCache=false;\n";
        jaasData += "};\n";
        try (FileOutputStream os = new FileOutputStream(file)) {
            os.write(jaasData.getBytes(Charset.forName("UTF-8")));
        }
        return file.getCanonicalPath();
    }
}
