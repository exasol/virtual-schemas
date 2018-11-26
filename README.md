# Virtual Schemas

[![Build Status](https://travis-ci.org/exasol/virtual-schemas.svg?branch=master)](https://travis-ci.org/exasol/virtual-schemas)

<p style="border: 1px solid black;padding: 10px; background-color: #FFFFCC;"><span style="font-size:200%">&#9888;</span> Please note that this is an open source project which is officially supported by Exasol. For any question, you can contact our support team.</p>

Virtual schemas provide a powerful abstraction to conveniently access arbitrary data sources. Virtual schemas are a kind of read-only link to an external source and contain virtual tables which look like regular tables except that the actual data are not stored locally.

After creating a virtual schema, its included tables can be used in SQL queries and even combined with persistent tables stored directly in Exasol, or with other virtual tables from other virtual schemas. The SQL optimizer internally translates the virtual objects into connections to the underlying systems and implicitly transfers the necessary data. SQL conditions are tried to be pushed down to the data sources to ensure minimal data transfer and optimal performance.

That's why this concept creates a kind of logical view on top of several data sources which could be databases or other data services. By that, you can either implement a harmonized access layer for your reporting tools or you can use this technology for agile and flexible ETL processing, since you don't need to change anything in Exasol if you change or extend the objects in the underlying system.

Please note that virtual schemas are part of the Advanced Edition of Exasol.

For further details about the concept, usage and examples, please see the corresponding chapter in our Exasol User Manual.

## API Specification

The subdirectory [doc](doc) contains the API specification for virtual schema adapters.

## JDBC Adapter

The subdirectory [jdbc-adapter](jdbc-adapter) contains the JDBC adapter which allows to integrate any kind of JDBC data source which provides a JDBC driver.

## Python Redis Demo Adapter

The subdirectory [python-redis-demo-adapter](python-redis-demo-adapter) contains a demo adapter for Redis written in Python. This adapter was created to easily demonstrate the key concepts in a real, but very simple implementation. If you want to write your own adapter, this might be the right code to get a first impression what you'll have to develop.