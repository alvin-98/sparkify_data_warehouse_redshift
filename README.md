# Redshift Data Warehouse for Music Streaming App
A music streaming startup has a large userbase and has data stored in an Amazon S3 instance. The project builds an ETL pipeline that moves data from S3 into a star schema table on Amazon Redshift which is optimized for queries on song plays.

## Tables

1. songplays - records in event data associated with song plays i.e. records with page 'NextSong' (Fact Table)
2. users - stores details of users of the app (Dimension Table)
3. songs - stores details of songs on the app (Dimension Table)
4. artists - stores details of artists whose music is on the app (Dimension Table)
5. time - stores timestamps of records in songplays broken down into specific units (Dimension Table)

## Instructions 

1. Create a new IAM Role and give is read only access to Amazon S3 buckets
2. Create a Redshift Cluster and make sure the security group is configured to allow public access to the cluster and allow all TCP requests
1. Run the 'create_tables.py' file to create all required tables
2. Next, run 'running etl.py' to execute data loading and transformation 
