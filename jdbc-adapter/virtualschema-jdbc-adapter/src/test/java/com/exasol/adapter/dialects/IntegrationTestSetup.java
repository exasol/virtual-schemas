package com.exasol.adapter.dialects;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

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
 * The main method of this class will be called in the pre-integration-test maven phase before the actual integration-test phase starst.
 * Here we have to setup the integration test environment
 *
 * Attention: This does not deploy the latest jar, because the dist maven module is cleaned and build after this module. Right now you need to do something like "mvn clean package && mvn verify -Pit -D..." to upload and test the latest jar.
 */
public class IntegrationTestSetup {

    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        if (args.length > 2) {
            Boolean skipTestSetup = Boolean.valueOf(args[2]);
            if (skipTestSetup) {
                System.out.println("Skip setup of the integration test environment");
                return;
            }
        }

        System.out.println("Start setup of the integration test environment");
        String projectVersion = args[0];
        String configFile = args[1];

        IntegrationTestConfig config = new IntegrationTestConfig(configFile);

        String bucketFSurl = config.getBucketFSURL();  
        String bucketFSpassword = config.getBucketFSPassword();
        		
        
        // The local path look like "virtualschema-jdbc-adapter-dist/target/virtualschema-jdbc-adapter-dist-1.0.2-SNAPSHOT.jar"
        String artifactDistName = "virtualschema-jdbc-adapter-dist";
        
        String jarName = artifactDistName + "-" + projectVersion + ".jar";
        
        String jarLocalPath = "../" + artifactDistName + "/target/" + jarName;
     
        
        uploadFileToBucketFS(bucketFSurl+"/"+jarName, jarLocalPath, bucketFSpassword);
        
        //uploadFileToBucketFS("http://192.168.106.131:2580/bucket1/original-virtualschema-jdbc-adapter-dist-1.0.2-SNAPSHOT.jar", "C:\\Users\\tb\\Desktop\\github-repos\\virtual-schemas\\jdbc-adapter\\virtualschema-jdbc-adapter-dist\\target\\original-virtualschema-jdbc-adapter-dist-1.0.2-SNAPSHOT.jar","bucket1");
                   
    }

  
    private static void uploadFileToBucketFS(String url, String filePath, String password) throws ClientProtocolException, IOException, URISyntaxException {
    	
    	HttpClient httpClient = HttpClientBuilder.create().build();
        URIBuilder uriBuilder = new URIBuilder(url);
        HttpPut request = new HttpPut(uriBuilder.build());
                
        String auth = "w:"+password;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("UTF-8")));
        String authHeader = "Basic " + new String(encodedAuth);
        request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
         
        FileEntity fileEntity = new FileEntity(new File(filePath));
        
        request.setEntity(fileEntity);
        
        HttpResponse response = httpClient.execute(request);

        if ( response.getStatusLine().getStatusCode() != 200 ) 
        	throw new IOException( response.toString() );
        
        System.out.println (response);
        
    }
    
    
}
