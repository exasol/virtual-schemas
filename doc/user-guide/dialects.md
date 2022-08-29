# Supported Dialects

Dialect name                              | OEM       | Repository                                                        | Latest release                                     |
------------------------------------------|-----------|-------------------------------------------------------------------|----------------------------------------------------|
[Athena][athena-dialect-doc]              | AWS       | [Athena Virtual Schema][athena-vs-repository]			 | [Latest release][athena-vs-releases]    |
[Aurora][aurora-dialect-doc]              | AWS       | [Virtual Schemas][virtual-schemas-repository]                    | [Latest release][virtual-schemas-releases]          |
[DB2][db2-dialect-doc]                    | IBM       | [DB2 Virtual Schemas][db2-virtual-schema-repository]             | [Latest release][db2-virtual-schema-releases]       |
[ElasticSearch][elasticsearch-dialect-doc] |          | [ElasticSearch][elasticsearch-repository] 			 | [Latest release][elasticsearch-releases]         |
[Exasol][exasol-dialect-doc]              | Exasol    | [Exasol Virtual Schema][exasol-vs-repository]        		 | [Latest release][exasol-vs-releases]    |
[Google Big Query][bigquery-dialect-doc]  | Google    | [Big Query Virtual Schema][bigquery-virtual-schema-repository]   | [Latest release][bigquery-virtual-schema-releases]  |
[HANA][hana-dialect-doc]                  | SAP       | [Hana Virtual Schemas][hana-vs-repository]                       | [Latest release][hana-vs-releases]      |
[Hive][hive-dialect-doc]                  | Apache    | [Hive Virtual Schemas][hive-vs-repository]                       | [Latest release][hive-vs-releases]      |
[Impala][impala-dialect-doc]              | Apache    | [Impala Virtual Schema][impala-vs-repository]        		 | [Latest release][impala-vs-releases]    |
[MySQL][mysql-dialect-doc]                |           | [MySQL Virtual Schema][mysql-vs-repository]          		 | [Latest release][mysql-vs-releases]     |
[Oracle][oracle-dialect-doc]              | Oracle    | [Oracle Virtual Schema][oracle-vs-repository]        		 | [Latest release][oracle-vs-releases]    |
[PostgreSQL][pg-dialect-doc]              |           | [PostgreSQL Virtual Schema][pg-vs-repository]        		 | [Latest release][pg-vs-releases]        |
[Redshift][redshift-dialect-doc]          | AWS       | [Redshift Virtual Schema][redshift-vs-repository]    		 | [Latest release][redshift-vs-releases]  |
[SQL Server][sql-server-dialect-doc]      | Microsoft | [SQL Server Virtual Schema][sqlserver-vs-repository] 		 | [Latest release][sqlserver-vs-releases] |
[Sybase ASE][sybase-dialect-doc]          | Sybase    | [Sybase Virtual Schema][sybase-vs-repository]        		 | [Latest release][sybase-vs-releases]    |
Generic                                   | Exasol    | [Virtual Schemas][virtual-schemas-repository]                    | [Latest release][virtual-schemas-releases]          |
Generic JDBC-capable RDBMS                | Exasol    | [Generic JDBC-capable RDBMS][jdbc-vs-repository]              	 | [Latest release][jdbc-vs-releases]                                  |
[Generic Document Files][document-vs-doc] | Exasol    | [Generic Document Files][document-vs-repository] 	      	 | [Latest release][document-vs-releases] 				       |

If your database is not part of that list but provides a JDBC driver, try to use the Generic driver.
You can also [develop a custom dialect][developing-dialect].

[aurora-dialect-doc]: https://github.com/exasol/virtual-schemas/blob/main/doc/dialects/aurora.md

[athena-dialect-doc]: https://github.com/exasol/athena-virtual-schema/blob/main/doc/user_guide/athena_user_guide.md
[athena-vs-releases]: https://github.com/exasol/athena-virtual-schema/releases
[athena-vs-repository]: https://github.com/exasol/athena-virtual-schema

[bigquery-dialect-doc]: https://github.com/exasol/bigquery-virtual-schema/blob/main/doc/user_guide/bigquery_user_guide.md
[bigquery-virtual-schema-releases]: https://github.com/exasol/bigquery-virtual-schema/releases
[bigquery-virtual-schema-repository]: https://github.com/exasol/bigquery-virtual-schema

[db2-dialect-doc]: https://github.com/exasol/db2-virtual-schema/blob/main/doc/user_guide/db2_user_guide.md
[db2-virtual-schema-releases]: https://github.com/exasol/db2-virtual-schema/releases
[db2-virtual-schema-repository]: https://github.com/exasol/db2-virtual-schema

[elasticsearch-dialect-doc]: https://github.com/exasol/elasticsearch-virtual-schema/blob/main/doc/user_guide/elasticsearch_sql_user_guide.md
[elasticsearch-releases]: https://github.com/exasol/elasticsearch-virtual-schema/releases
[elasticsearch-repository]: https://github.com/exasol/elasticsearch-virtual-schema

[exasol-dialect-doc]: https://github.com/exasol/exasol-virtual-schema/blob/master/doc/dialects/exasol.md
[exasol-vs-releases]: https://github.com/exasol/exasol-virtual-schema/releases
[exasol-vs-repository]: https://github.com/exasol/exasol-virtual-schema

[hive-dialect-doc]: https://github.com/exasol/hive-virtual-schema/blob/main/doc/user_guide/hive_user_guide.md
[hive-vs-releases]: https://github.com/exasol/hive-virtual-schema/releases
[hive-vs-repository]: https://github.com/exasol/hive-virtual-schema

[impala-dialect-doc]: https://github.com/exasol/impala-virtual-schema/blob/main/doc/user_guide/impala_user_guide.md
[impala-vs-releases]: https://github.com/exasol/impala-virtual-schema/releases
[impala-vs-repository]: https://github.com/exasol/impala-virtual-schema

[mysql-dialect-doc]: https://github.com/exasol/mysql-virtual-schema/blob/main/doc/user_guide/mysql_user_guide.md
[mysql-vs-releases]: https://github.com/exasol/mysql-virtual-schema/releases
[mysql-vs-repository]: https://github.com/exasol/mysql-virtual-schema

[oracle-dialect-doc]: https://github.com/exasol/oracle-virtual-schema/blob/main/doc/user_guide/oracle_user_guide.md
[oracle-vs-releases]: https://github.com/exasol/oracle-virtual-schema/releases
[oracle-vs-repository]: https://github.com/exasol/oracle-virtual-schema

[pg-vs-releases]: https://github.com/exasol/postgresql-virtual-schema/releases
[pg-vs-repository]: https://github.com/exasol/postgresql-virtual-schema
[pg-dialect-doc]: https://github.com/exasol/postgresql-virtual-schema/blob/main/doc/user_guide/postgresql_user_guide.md

[redshift-dialect-doc]: https://github.com/exasol/redshift-virtual-schema/blob/main/doc/user_guide/redshift_user_guide.md
[redshift-vs-releases]: https://github.com/exasol/redshift-virtual-schema/releases
[redshift-vs-repository]: https://github.com/exasol/redshift-virtual-schema

[hana-dialect-doc]:  https://github.com/exasol/hana-virtual-schema/blob/main/doc/user_guide/user_guide.md
[hana-vs-releases]: https://github.com/exasol/hana-virtual-schema/releases
[hana-vs-repository]: https://github.com/exasol/hana-virtual-schema

[sql-server-dialect-doc]: https://github.com/exasol/sqlserver-virtual-schema/blob/main/doc/user_guide/sqlserver_user_guide.md
[sqlserver-vs-releases]: https://github.com/exasol/sqlserver-virtual-schema/releases
[sqlserver-vs-repository]: https://github.com/exasol/sqlserver-virtual-schema

[sybase-dialect-doc]: https://github.com/exasol/sybase-virtual-schema/blob/main/doc/user_guide/sybase_user_guide.md
[sybase-vs-releases]: https://github.com/exasol/sybase-virtual-schema/releases
[sybase-vs-repository]: https://github.com/exasol/sybase-virtual-schema



<!-- no [virtual-schemas-dialect-doc] -->
[virtual-schemas-releases]: https://github.com/exasol/virtual-schemas/releases
[virtual-schemas-repository]: https://github.com/exasol/virtual-schemas

<!-- no [jdbc-dialect-doc], no user guide -->
[jdbc-vs-releases]: https://github.com/exasol/virtual-schema-common-jdbc/releases
[jdbc-vs-repository]: https://github.com/exasol/virtual-schema-common-jdbc

<!-- no [jdbc-dialect-doc], no user guide -->
[jdbc-vs-releases]: https://github.com/exasol/virtual-schema-common-jdbc/releases
[jdbc-vs-repository]: https://github.com/exasol/virtual-schema-common-jdbc

[document-vs-doc]: https://github.com/exasol/virtual-schema-common-document-files/blob/main/doc/user_guide/user_guide.md
[document-vs-releases]: https://github.com/exasol/virtual-schema-common-document-files/releases
[document-vs-repository]: https://github.com/exasol/virtual-schema-common-document-files

[developing-dialect]: https://github.com/exasol/virtual-schema-common-jdbc/blob/main/doc/development/developing_a_dialect.md

