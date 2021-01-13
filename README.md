# Big_Data_Project_ETL_Process
Requirements
Use the raw data of online streaming video data to construct a star schema Data Warehouse which will be used to track VideoStarts over time.
Show the SQL queries you would use to populate the Data Warehouse Dimensions and Fact table.
Stages involved in the ETL Process
1.	Design the star schema.
2.	Create a database, schema and a source table in Snowflake. The source table is used to save the raw data.
3.	Create a file format specifically for the csv raw data file.
4.	Create a stage in Snowflake to connect with the S3 bucket where the raw data will be loaded into.
5.	Create a snowpipe in Snowflake using ‘auto_ingest=true’, so the raw data in S3 can be automatically copied from S3 bucket into the source table in Snowflake. A SQS event notification is required to be setup in S3, so that when the data file arrives S3 will trigger a notification to Snowflake.
6.	Once the raw data is loaded into the source table, validate the data to ensure the extraction process is completed correctly.
7.	Perform data transformation and save the result into a view: 
  *	Only select records when events contain ‘206’ and VideoTitle column has value
  *	Split the platform name from videoTitle using the first element of the VideoTitle string
  *	Split the site name from videoTile, also by using the first element of the VideoTile string
  *	Split year, quarter, month, week, day of week, hour, and minute from DateTime
8.	Based on the designed star schema, create the dimension tables and ingest data into dimension tables. 
9.	Ingest data into fact table.
