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
        if (pass==null) {
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
        String krbConfPath = writeKrbConf(krbDir, confKeytab[0]);
        String keytabPath = writeKeytab(krbDir, confKeytab[1]);
        String jaasConfigPath = writeJaasConfig(krbDir, user, keytabPath);
        System.setProperty("java.security.auth.login.config", jaasConfigPath);
        System.setProperty("java.security.krb5.conf", krbConfPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    }

    private static String writeKrbConf(File krbDir, String base64Conf) throws Exception {
        File file = File.createTempFile("krb_", ".conf", krbDir);
        file.deleteOnExit();
        FileOutputStream os = new FileOutputStream(file);
        os.write(DatatypeConverter.parseBase64Binary(base64Conf));
        os.close();
        return file.getCanonicalPath();
    }

    private static String writeKeytab(File krbDir, String base64Keytab) throws Exception {
        File file = File.createTempFile("kt_", ".keytab", krbDir);
        file.deleteOnExit();
        FileOutputStream os = new FileOutputStream(file);
        os.write(DatatypeConverter.parseBase64Binary(base64Keytab));
        os.close();
        return file.getCanonicalPath();
    }

    private static String writeJaasConfig(File krbDir, String princ, String keytabPath) throws Exception {
        File file = File.createTempFile("jaas_", ".conf", krbDir);
        file.deleteOnExit();
        String jaasData;
        jaasData  = "Client {\n";
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
        FileOutputStream os = new FileOutputStream(file);
        os.write(jaasData.getBytes(Charset.forName("UTF-8")));
        os.close();
        return file.getCanonicalPath();
    }
}
