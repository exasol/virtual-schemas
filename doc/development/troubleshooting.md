# Troubleshooting

This article covers known pitfalls and explains steps you can take to solve them.

## JDBC Driver

### Error: No suitable driver found for JDBC...

The JDBC driver class was not discovered automatically. Either you have to add a `META-INF/services/java.sql.Driver` file with the class name to your JAR, 
or you have to load the driver manually (see `JdbcMetadataReader.readRemoteMetadata()`).

See also:

* https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html

## Performance

## Low Performance due to Log Output

If you use [remote logging](remote_logging.md), a number of factors can slow down the execution of a Virtual Schema.

Those are the things you can do to improve performance:

* Make sure there is a fast network connection between the cluster nodes running the virtual schema and the machine receiving the log
* Lower the `DEBUG_LEVEL` to `INFO` or `WARNING`
* Disable remote logging

### Low Performance Caused by Slow Randomness Source

Depending on which JDK version Exasol uses to execute Java user-defined functions, a blocking random-number source may be used by default. 

Especially cryptographic operations do not complete until the operating system has collected a sufficient amount of entropy (read "real random values").

This problem mostly occurs when Exasol is run in an isolated environment, typically a virtual machine or a container.

#### Option a) Run a Process in Parallel That Generates Entropy

Operating systems use various sources of random data input, like keystroke timing, disk seeks and network timing. You can increase entropy by running processes in parallel that feed the entropy collection. Which ones those are depends on the OS. 

#### Option b) Install Drivers That get Entropy From the Host's Hardware

Especially server machines often have dedicated hardware entropy sources. Still commodity hardware parts like sound adapters can be repurposed to create randomness, e.g. form random noise of an analog input.
In order to utilize those in virtual machines you usually need drivers and / or guest extensions that allow reading random data from the host.

#### Option c) Dangerous: Using a Pseudo-random Source

Since randomness is usually used for security measures like cryptography, using pseudo-random data is dangerous! Pseudo-random is another word for "guessable" and that is not what you want for cryptography.

If you intend to use this option, then do it **only for integration tests with non-confidential data**

* Log in to EXAOperation and shutdown the database. 
* Append `-etlJdbcJavaEnv -Djava.security.egd=/dev/urandom` to the "Extra Database Parameters" input field and power the database on again.


### Setting the Right IP Addresses for Database Connections

Keep in mind that the adapter script is deployed in the Exasol database. 
If you want it to be able to make connections to other databases, you need to make sure that the IP addresses or host names are the ones that the database sees, 
not your local machine. This is easily forgotten in case of automated integration tests since it feels like they run on your machine -- which is only partially true.

So a common source of error would be to specify `localhost` or `127.0.0.1` as address of the remote database 
in case you have it running in Docker or a VM on your local machine. 
But the Exasol Database cannot reach the other database there unless it is running on the same machine directly (i.e. not behind a virtual network device).

