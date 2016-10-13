package com.exasol.adapter.dialects;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

/**
 * The main method of this class will be called in the pre-integration-test maven phase before the actual integration-test phase starst.
 * Here we have to setup the integration test environment
 *
 * Attention: This does not deploy the latest jar, because the dist maven module is cleaned and build after this module. Right now you need to do something like "mvn clean package && mvn verify -Pit -D..." to upload and test the latest jar.
 */
public class IntegrationTestSetup {

    public static void main(String[] args) throws IOException, InterruptedException {

        System.out.println("Start setup of the integration test environment");
        String projectVersion = args[0];
        String configFile = args[1];

        IntegrationTestConfig config = new IntegrationTestConfig(configFile);

        // The local path look like "virtualschema-jdbc-adapter-dist/target/virtualschema-jdbc-adapter-dist-0.0.1-SNAPSHOT.jar"
        String artifactDistName = "virtualschema-jdbc-adapter-dist";
        String scpLocalPath = "../" + artifactDistName + "/target/" + artifactDistName + "-" + projectVersion + ".jar";
        String scpTargetPath = config.getScpTargetPath();
        ImmutableList<String> commands = ImmutableList.of("scp", scpLocalPath, scpTargetPath);
        runBashCommand(commands);
    }

    private static void runBashCommand(List<String> commands) throws IOException, InterruptedException {
        System.out.println("EXECUTE command: " + commands);
        ProcessBuilder pb = new ProcessBuilder(commands).inheritIO();
        Process process = pb.start();
        process.waitFor();
        System.out.println("Process ended with exit value " + process.exitValue());
        if (process.exitValue() != 0) {
            throw new RuntimeException("SCP failed.");
        }
    }

}
