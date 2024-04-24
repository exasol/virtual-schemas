# Supported Dialects

| Dialect name                                                                       | OEM       | Type      | Repository                                                                                               | Latest release                                             |
|------------------------------------------------------------------------------------|-----------|-----------|----------------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| [Athena][athena-dialect-doc]                                                       | AWS       | JDBC      | [Athena Virtual Schema][athena-vs-repository]                                                            | [Latest release][athena-vs-releases]                       |
| [Aurora][aurora-dialect-doc]                                                       | AWS       | JDBC      | [Virtual Schemas][virtual-schemas-repository]                                                            | [Latest release][virtual-schemas-releases]                 |
| [Azure Blob Storage document files][azure-blob-storage-vs-doc]                     | Microsoft | document  | [Azure Blob Storage document files Virtual Schema][azure-blob-storage-vs-repository]                     | [Latest release][azure-blob-storage-vs-releases]           |
| [Azure Data Lake Storage Gen2 document files][azure-data-lake-storage-gen2-vs-doc] | Microsoft | document  | [Azure Data Lake Storage Gen2 document files Virtual Schema][azure-data-lake-storage-gen2-vs-repository] | [Latest release][azure-data-lake-storage-gen2-vs-releases] |
| [BucketFS document files][bucketfs-vs-doc]                                         | Exasol    | document  | [BucketFS document files Virtual Schema][bucketfs-vs-repository]                                         | [Latest release][bucketfs-vs-releases]                     |
| [DB2][db2-dialect-doc]                                                             | IBM       | JDBC      | [DB2 Virtual Schema][db2-virtual-schema-repository]                                                     | [Latest release][db2-virtual-schema-releases]              |
| [Dynamo DB][dynamodb-vs-doc]                                                       | AWS       | document  | [Dynamo DB Virtual Schema][dynamodb-vs-repository]                                                       | [Latest release][dynamodb-vs-releases]                     |
| [ElasticSearch][elasticsearch-dialect-doc]                                         |           | JDBC      | [ElasticSearch][elasticsearch-repository]                                                                | [Latest release][elasticsearch-releases]                   |
| [Exasol][exasol-dialect-doc]                                                       | Exasol    | JDBC *)   | [Exasol Virtual Schema][exasol-vs-repository]                                                            | [Latest release][exasol-vs-releases]                       |
| [Google Big Query][bigquery-dialect-doc]                                           | Google    | JDBC      | [Big Query Virtual Schema][bigquery-virtual-schema-repository]                                           | [Latest release][bigquery-virtual-schema-releases]         |
| [Google Cloud Storage document files][google-cloud-storage-vs-doc]                 | Google    | document  | [Google Cloud Storage document files Virtual Schema][google-cloud-storage-vs-repository]                 | [Latest release][google-cloud-storage-vs-releases]         |
| [HANA][hana-dialect-doc]                                                           | SAP       | JDBC      | [Hana Virtual Schemas][hana-vs-repository]                                                               | [Latest release][hana-vs-releases]                         |
| [Hive][hive-dialect-doc]                                                           | Apache    | JDBC      | [Hive Virtual Schemas][hive-vs-repository]                                                               | [Latest release][hive-vs-releases]                         |
| [Impala][impala-dialect-doc]                                                       | Apache    | JDBC      | [Impala Virtual Schema][impala-vs-repository]                                                            | [Latest release][impala-vs-releases]                       |
| [MySQL][mysql-dialect-doc]                                                         |           | JDBC      | [MySQL Virtual Schema][mysql-vs-repository]                                                              | [Latest release][mysql-vs-releases]                        |
| [Oracle][oracle-dialect-doc]                                                       | Oracle    | JDBC      | [Oracle Virtual Schema][oracle-vs-repository]                                                            | [Latest release][oracle-vs-releases]                       |
| [PostgreSQL][pg-dialect-doc]                                                       |           | JDBC      | [PostgreSQL Virtual Schema][pg-vs-repository]                                                            | [Latest release][pg-vs-releases]                           |
| [Redshift][redshift-dialect-doc]                                                   | AWS       | JDBC      | [Redshift Virtual Schema][redshift-vs-repository]                                                        | [Latest release][redshift-vs-releases]                     |
| [S3 document files][s3-vs-doc]                                                     | AWS       | document  | [S3 document files Virtual Schema][s3-vs-repository]                                                     | [Latest release][s3-vs-releases]                           |
| [SQL Server][sql-server-dialect-doc]                                               | Microsoft | JDBC      | [SQL Server Virtual Schema][sqlserver-vs-repository]                                                     | [Latest release][sqlserver-vs-releases]                    |
| [Sybase ASE][sybase-dialect-doc]                                                   | Sybase    | JDBC      | [Sybase Virtual Schema][sybase-vs-repository]                                                            | [Latest release][sybase-vs-releases]                       |
| Generic                                                                            | Exasol    | (generic) | [Virtual Schemas][virtual-schemas-repository]                                                            | [Latest release][virtual-schemas-releases]                 |
| Generic JDBC-capable RDBMS                                                         | Exasol    | JDBC      | [Generic JDBC-capable RDBMS][jdbc-vs-repository]                                                         | [Latest release][jdbc-vs-releases]                         |
| [Generic Document Files][document-vs-doc]                                          | Exasol    | document  | [Generic Document Files][document-vs-repository]                                                         | [Latest release][document-vs-releases]                     |

\*) The Virtual Schema for Exasol databases supports three connection variants in total:
* Import from JDBC
* Using EXA Import
* Using `IS_LOCAL`

If your database is not part of that list but provides a JDBC driver, try to use the Generic driver. You can also [develop a custom dialect][developing-dialect].

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

[dynamodb-vs-doc]: https://github.com/exasol/dynamodb-virtual-schema/blob/main/doc/user-guide/user_guide.md
[dynamodb-vs-releases]: https://github.com/exasol/dynamodb-virtual-schema/releases
[dynamodb-vs-repository]: https://github.com/exasol/dynamodb-virtual-schema

[azure-blob-storage-vs-doc]: https://github.com/exasol/azure-blob-storage-document-files-virtual-schema/blob/main/doc/user_guide/user_guide.md
[azure-blob-storage-vs-releases]: https://github.com/exasol/azure-blob-storage-document-files-virtual-schema/releases
[azure-blob-storage-vs-repository]: https://github.com/exasol/azure-blob-storage-document-files-virtual-schema

[azure-data-lake-storage-gen2-vs-doc]: https://github.com/exasol/azure-data-lake-storage-gen2-document-files-virtual-schema/blob/main/doc/user_guide/user_guide.md
[azure-data-lake-storage-gen2-vs-releases]: https://github.com/exasol/azure-data-lake-storage-gen2-document-files-virtual-schema/releases
[azure-data-lake-storage-gen2-vs-repository]: https://github.com/exasol/azure-data-lake-storage-gen2-document-files-virtual-schema

[google-cloud-storage-vs-doc]: https://github.com/exasol/google-cloud-storage-document-files-virtual-schema/blob/main/doc/user_guide/user_guide.md
[google-cloud-storage-vs-releases]: https://github.com/exasol/google-cloud-storage-document-files-virtual-schema/releases
[google-cloud-storage-vs-repository]: https://github.com/exasol/google-cloud-storage-document-files-virtual-schema

[s3-vs-doc]: https://github.com/exasol/s3-document-files-virtual-schema/blob/main/doc/user_guide/user_guide.md
[s3-vs-releases]: https://github.com/exasol/s3-document-files-virtual-schema/releases
[s3-vs-repository]: https://github.com/exasol/s3-document-files-virtual-schema

[bucketfs-vs-doc]: https://github.com/exasol/bucketfs-document-files-virtual-schema/blob/main/doc/user_guide/user_guide.md
[bucketfs-vs-releases]: https://github.com/exasol/bucketfs-document-files-virtual-schema/releases
[bucketfs-vs-repository]: https://github.com/exasol/bucketfs-document-files-virtual-schema

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

