# Observing Adapter Output

You can either use [netcat](http://netcat.sourceforge.net/) or `exaoutput.py` from the [EXASolution Python Package](https://github.com/EXASOL/python-exasol). 
Since netcat is available on most Linux machines anyway, we will use this in the description here.

First start netcat in listen-mode on a free TCP port on your machine.

```bash
nc -lkp 3000
```

The `-l` switch puts netcat into listen-mode. `-k` tells it to stay open after the peer closed a connection. `-p 3000` set the number of the TCP port netcat listens on.

Next find out your IP address.

Linux:

```bash
ip -br address
```

Windows:

```cmd
ipconfig /all
```

The next SQL command shows an example of declaring a virtual schema. Notice the IP address and port in the last line. This tells the adapter script where to direct the output to. 

```sql
CREATE VIRTUAL SCHEMA VS_EXA_IT
USING ADAPTER.JDBC_ADAPTER
WITH CONNECTION_NAME='EXASOL_CONNECTION'
     SCHEMA_NAME='NATIVE_EXA_IT' SQL_DIALECT='EXASOL' IS_LOCAL='true'
     DEBUG_ADDRESS='10.44.1.228:3000' LOG_LEVEL='ALL';
```

The parameter LOG_LEVEL lets you pick a log level as defined in [java.util.logging.Level](https://docs.oracle.com/javase/8/docs/api/java/util/logging/Level.html).

The recommended standard log levels are:

* `INFO` in production
* `ALL` for in-depth debugging

You can tell that the connection works if you see the following message after executing the SQL command that installs a virtual schema:

    Attached to output service