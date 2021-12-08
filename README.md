# wcdsb_ods

Aspen ODS replication using the data dictionary to rename fields and only pull active and system fields.  

This project was started before the offical ODS was release and migrated from python scripts to use T-SQL.

# Introduction 
Replication SIS data locally using the data dictionary to rename and select fields.

# Getting Started

1. Aspen_ODS_init.sql       - sets up basic tables
2. Procs and Functions.sql  - creates procs and functions thats sp_multitable_load needs
3. sp_multiTable_Load.sql   - main script to extract data from the link server


* Initialize all tables for Compass ETL: `exec sp_multiTable_Load @tables = 'Compass'`
* Initialize all tables with no data: `exec sp_multiTable_Load @tables = 'init'`
* Initialize all tables with top 10 data: `exec sp_multiTable_Load @tables = 'TOP10'`
* Initialize all tables with ALL data: `exec sp_multiTable_Load @tables = 'FULL'`

Test extract: `exec sp_multiTable_Load @tables = 'school,district_school_year_context,person'`
               

# SQL Job

Create a new SQL job that runs the following proc  `exec sp_Aspen_Replication_Interval @interval = 'daily', @debug =0` fill in `dbo.aspen_replication` with any additional tables.
