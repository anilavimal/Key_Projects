Data set used
--------------
1.Customer.csv
2.Order.csv

 Services Used : Amazon S3, Snowflake, Amazon MWAA, Amazon Kinesis Firehose, AWS EC2
 
•	ON EC2 
  . Install kinesis agent
  . Place dataset on EC2 machine.
  •	While emitting logs, it is getting captured by Amazon Kinesis Agent and sent to Firehose delivery stream
  . Devivery stream should pass this data to S3
  . Start kinesis agent - Sudo service aws-kinesis-agent-start
  . Tail -f /var/log/ aws-kinesis-agent/ aws-kinesis-agent.log


•	On Firehose 
    o	Created Delivery streams
    o	Destination S3 : firehose/customers/landing
    
•	data sent to 3 zones.
  o	Landing zone
  o	Processing zone
  o	Processed zone 
  
•	On Snowflake
  . Set up snowflake database
  o	Create DB
  o	Create schema – “SCHEMA1”
  o	Create Warehouse	
  o	Create roles:
    	Account -> Roles -> DEVELOPER_ROLE1
  o	Grant privileges on DB
    	Create schema, Modify, Monitor, Usage
    	Grant to: DEVELOPER_ROLE1
    •	Create table:
  . SCHEMA1 -Create table ->name: “Customers"
  . SCHEMA1 -Create table ->name: “Orders"
   
 •	Create storage Integration
 •	Setup snowflake stages:

How do we move data in S3 to Snowflake?
  . Create DAG using Airflow

AWS Managed Apache Airflow
=====================

3 steps:
  1.	Create Environment
  2.	Upload DAG to S3
  3.	Run DAG in Airflow

•	Apache Airflow DAG  
  o	Move data from Landing to Processing zone: firehose/customers/processing
  o	It will trigger Snowflake for ingestion and transformation.
  o	After Transformation move data to Processed folder :firehose/customers/processed
  
. Airflow uses S3 to load dags and supporting files.

.Create 2 filder in S3 bucket as below
  •	dag
  •	requirement
. Upload DAG python script.py in the folder "dag" .
. Upload requiremnt.txt to requirement folder. 

. Create Airflow Environment
  •	Environment class :Airflow Cluster
  •	Max Worker count:5 (must be between 1 and 25)
  •	Min Worker count:1
  •	Scheduler count:2. (must be between 1 and 5)
. Networking:
  •	Select VPC and subnet create now.
  •	Select public network
  •	Create new Security Group

. Create VPC
  •	Airflow runs in VPC
  •	Create AWS managed Airflow VPC - It will take to CloudFormation console -Create stack
  
. Steps to connect Airflow to Snowflake Environment 
  •	Airflow UI ->Admin-Connections 
  •	Create connection 
  •	Host: Go to snowflake UI login -Copy URL Paste above host 
  •	Snowflake Username and password
  •	Extra: { account: warehouse: database: region: }

. Define tasks. DAG.py
. Create pipeline
  . create the pipeline by adding the “>>” operator between the tasks.
  . t1 >> t2

  
