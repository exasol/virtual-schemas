package com.exasol.adapter.dialects;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.logging.Logger;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * The main method of this class will be called in the
 * <code>pre-integration-test</code> Maven phase before the actual
 * <code>integration-test</code> phase starts.
 *
 * <p>
 * We upload the JAR file containing the virtual schema adapter to BucketFS so
 * that we don't accidentally forget to do this before the integration test.
 * There is a caveat though: if you run this on a cluster, the cluster nodes
 * will not replicate the JAR instantly. This takes a while. So in clustered
 * environments it is safer to upload the file and wait a while.
 * </p>
 *
 * <p>
 * <strong>Attention:</strong>
 * </p>
 *
 * <p>
 * This does not deploy the latest JAR, because the distribution Maven module is
 * and and build after this module. Right now you need to do something like
 * <code><pre>mvn clean package && mvn verify -Pit -D...</pre></code> to upload
 * and test the latest JAR.
 * </p>
 */
public class IntegrationTestSetup {
    private static final String ARTIFACT_DISTRIBUTION_NAME = "virtualschema-jdbc-adapter-dist";
    private static final Logger LOGGER = Logger.getLogger(IntegrationTestConfig.class.getName());
    private final IntegrationTestConfig config;
    private final String version;

    public static void main(final String[] args) throws ClientProtocolException, IOException, URISyntaxException {
        if (isSkippingIntegrationTestConfigured(args)) {
            LOGGER.info("Skipping setup of the integration test environment");
        } else {
            LOGGER.info("Setting up the integration test environment");
            final String projectVersion = args[0];
            final String configFile = args[1];
            new IntegrationTestSetup(new IntegrationTestConfig(configFile), projectVersion).run();
        }
    }

    private static boolean isSkippingIntegrationTestConfigured(final String[] args) {
        return args.length > 2 && Boolean.valueOf(args[2]);
    }

    private void run() throws ClientProtocolException, IOException, URISyntaxException {
        uploadFileToBucketFS(getJarUrlForBucketFS(this.config.getBucketFSURL()), //
                getLocalJarPath(), //
                this.config.getBucketFSPassword());
    }

    private IntegrationTestSetup(final IntegrationTestConfig config, final String version) {
        this.config = config;
        this.version = version;
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

    private void uploadFileToBucketFS(final String url, final String filePath, final String password)
            throws ClientProtocolException, IOException, URISyntaxException {
        LOGGER.info(() -> "Uploading \"" + filePath + "\"" + " to \"" + url + "\"");
        final HttpPut request = buildPutRequest(url, filePath, password);
        final HttpResponse response = executePutRequest(request);
        handleResponse(response);
        LOGGER.fine(() -> "HTTP PUT response:" + System.lineSeparator() + response);
    }

    private void handleResponse(final HttpResponse response) throws IOException {
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException(response.toString());
        }
    }

    private HttpResponse executePutRequest(final HttpPut request) throws IOException, ClientProtocolException {
        final HttpClient httpClient = HttpClientBuilder.create().build();
        final HttpResponse response = httpClient.execute(request);
        return response;
    }

    private HttpPut buildPutRequest(final String url, final String filePath, final String password)
            throws URISyntaxException {
        final URIBuilder uriBuilder = new URIBuilder(url);
        final HttpPut request = new HttpPut(uriBuilder.build());
        final String auth = "w:" + password;
        final byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("UTF-8")));
        final String authHeader = "Basic " + new String(encodedAuth);
        request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        final FileEntity fileEntity = new FileEntity(new File(filePath));
        request.setEntity(fileEntity);
        return request;
    }
}