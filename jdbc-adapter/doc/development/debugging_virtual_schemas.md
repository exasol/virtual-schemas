# Frequent Issues

### Error: No suitable driver found for JDBC...

The JDBC driver class was not discovered automatically. Either you have to add a `META-INF/services/java.sql.Driver` file with the class name to your JAR, 
or you have to load the driver manually (see `JdbcMetadataReader.readRemoteMetadata()`).

See https://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html

### Very Slow Execution of Queries With SCRIPT_OUTPUT_ADDRESS

<!--If `SCRIPT_OUTPUT_ADDRESS` is set as explained in the [debugging section](#debugging), verify that a service is actually listening at that address. -->
<!--Otherwise, if Exasol can not establish a connection, repeated connection attempts can be the cause for slowdowns.-->
<!-- TODO -->

### Very Slow Execution of Queries

Depending on which JDK version Exasol uses to execute Java user-defined functions, a blocking random-number source may be used by default. 
Especially cryptographic operations do not complete until the operating system has collected a sufficient amount of entropy. 
This problem seems to occur most often when Exasol is run in an isolated environment, e.g., a virtual machine or a container. 
A solution is to use a non-blocking random-number source. 

To do so, log in to EXAOperation and shutdown the database. 
Append `-etlJdbcJavaEnv -Djava.security.egd=/dev/./urandom` to the "Extra Database Parameters" input field and power the database on again.

### Setting the Right IP Addresses for Database Connections

Keep in mind that the adapter script is deployed in the Exasol database. 
If you want it to be able to make connections to other databases, you need to make sure that the IP addresses or host names are the ones that the database sees, 
not your local machine. This is easily forgotten in case of automated integration tests since it feels like they run on your machine -- which is only partially true.

So a common source of error would be to specify `localhost` or `127.0.0.1` as address of the remote database 
in case you have it running in Docker or a VM on your local machine. 
But the Exasol Database cannot reach the other database there unless it is running on the same machine directly (i.e. not behind a virtual network device).

