package com.exasol.adapter.jdbc;

public class ConnectionInformation {
    private String credentials;
    private String exaConnectionString;
    private String oraConnectionName;

    public ConnectionInformation(String credentials, String exaConnectionString, String oraConnectionName) {
        this.credentials = credentials;
        this.exaConnectionString = exaConnectionString;
        this.oraConnectionName = oraConnectionName;
    }

    public String getCredentials() {
        return credentials;
    }

    public String getExaConnectionString() {
        return exaConnectionString;
    }

    public String getOraConnectionName() {
        return oraConnectionName;
    }
}
