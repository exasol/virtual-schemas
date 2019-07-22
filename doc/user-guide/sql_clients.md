# SQL Client Specifics

This section covers specifics of SQL client that are good to know when you are setting up Virtual Schemas or are working with them.

## DBeaver

DBeaver has trouble telling where scripts end that contain multiple semicolons or newlines. The simplest way to get around that problem is to mark the whole script and execute it as a single step.

## DbVisualizer

### Delimiting Scripts in DB Visualizer

To tell DbVisualizer that a part of a script should be handled as a single statement, you can insert an SQL block begin-identifier just before the block and an end-identifier after the block.

The delimiter must be the only text on the line. The default begin-identifier consists of two dashes followed by a forward slash (`--/`) and for the End Identifier it is a single slash (`/`).