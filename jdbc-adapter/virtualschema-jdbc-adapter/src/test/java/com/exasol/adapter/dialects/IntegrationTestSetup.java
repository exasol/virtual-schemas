package com.exasol.adapter.dialects;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * The main method of this class will be called in the <code>pre-integration-test</code> Maven phase before the actual
 * <code>integration-test</code> phase starts.
 *
 * <p>
 * We upload the JAR file containing the virtual schema adapter to BucketFS so that we don't accidentally forget to do
 * this before the integration test. There is a caveat though: if you run this on a cluster, the cluster nodes will not
 * replicate the JAR instantly. This takes a while. So in clustered environments it is safer to upload the file and wait
 * a while.
 * </p>
 *
 * <p>
 * <strong>Attention:</strong>
 * </p>
 *
 * <p>
 * This does not deploy the latest JAR, because the distribution Maven module is and and build after this module. Right
 * now you need to do something like <code><pre>mvn clean package && mvn verify -Pit -D...</pre></code> to upload and
 * test the latest JAR.
 * </p>
 */
public class IntegrationTestSetup {
    private static final String ARTIFACT_DISTRIBUTION_NAME = "virtualschema-jdbc-adapter-dist";
    private static final Logger LOGGER = Logger.getLogger(IntegrationTestConfig.class.getName());
    private IntegrationTestConfig config = null;
    private final String version;
    private final String configFile;

    /**
     * Entry point of the {@link IntegrationTestSetup}
     *
     * @param args version of the adapter, path to configuration file and skipping (optional)
     */
    public static void main(final String[] args) {
        if (isSkippingIntegrationTestConfigured(args)) {
            LOGGER.info("Skipping setup of the integration test environment");
        } else {
            LOGGER.info("Setting up the integration test environment");
            final String projectVersion = args[0];
            final String configFile = args[1];
            new IntegrationTestSetup(configFile, projectVersion).run();
        }
    }

    private IntegrationTestSetup(final String configFile, final String version) {
        this.configFile = configFile;
        this.version = version;
    }

    private static boolean isSkippingIntegrationTestConfigured(final String[] args) {
        return (args.length > 2) && Boolean.valueOf(args[2]);
    }

    private void run() {
        readConfiguration();
        uploadFileToBucketFS(getJarUrlForBucketFS(this.config.getBucketFSURL()), //
                getLocalJarPath(), //
                this.config.getBucketFSPassword());
    }

    private void readConfiguration() {
        try {
            this.config = new IntegrationTestConfig(this.configFile);
        } catch (final FileNotFoundException e) {
            throw new IntegrationTestSetupException(
                    "Unable to read integration test configuration file \"" + this.configFile + "\"", e);
        }
    }

    private String getJarUrlForBucketFS(final String bucketFSurl) {
        return bucketFSurl + "/" + getJarName(this.version);
    }

    private String getLocalJarPath() {
        return ".." + File.separator + ARTIFACT_DISTRIBUTION_NAME + File.separator + "target" + File.separator
                + getJarName(this.version);
    }

    private String getJarName(final String projectVersion) {
        return ARTIFACT_DISTRIBUTION_NAME + "-" + projectVersion + ".jar";
    }

    private void uploadFileToBucketFS(final String url, final String filePath, final String password) {
        LOGGER.info(() -> "Uploading \"" + filePath + "\"" + " to \"" + url + "\"");
        final HttpPut request = buildPutRequest(url, filePath, password);
        final HttpResponse response = executePutRequest(request);
        handleResponse(response);
        LOGGER.fine(() -> "HTTP PUT response:" + System.lineSeparator() + response);
    }

    private void handleResponse(final HttpResponse response) {
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IntegrationTestSetupException("HTTP PUT request to BucketFS failed: " + response.toString());
        }
    }

    private HttpResponse executePutRequest(final HttpPut request) {
        try {
            final HttpClient httpClient = HttpClientBuilder.create().build();
            HttpResponse response;
            response = httpClient.execute(request);
            return response;
        } catch (final IOException e) {
            throw new IntegrationTestSetupException("Unable to execute HTTP PUT to BucketFS", e);
        }
    }

    private HttpPut buildPutRequest(final String url, final String filePath, final String password) {
        try {
            final URIBuilder uriBuilder = new URIBuilder(url);
            final HttpPut request = new HttpPut(uriBuilder.build());
            setAuthenticationHeaderInRequestForPassword(request, password);
            setFileToBeTransferred(request, filePath);
            return request;
        } catch (final URISyntaxException e) {
            throw new IntegrationTestSetupException(
                    "Unable to build HTTP PUT request from \"" + filePath + "\" to \"" + url + "\"", e);
        }
    }

    private void setAuthenticationHeaderInRequestForPassword(final HttpPut request, final String password) {
        final String auth = "w:" + password;
        final byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("UTF-8")));
        final String authHeader = "Basic " + new String(encodedAuth);
        request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }

    private void setFileToBeTransferred(final HttpPut request, final String filePath) {
        final FileEntity fileEntity = new FileEntity(new File(filePath));
        request.setEntity(fileEntity);
    }
}