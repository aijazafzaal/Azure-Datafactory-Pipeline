After creating the AWS S3 bucket and loading the data to S3 buckets, the data from AWS S3 buckets is moved to containers in Azure Blob Storage  by using <b> Azure Data Factory Pipeline</b> and mounted on <b>Azure Databricks</b>.  
The file is converted into a table in Databricks and analysis is performed using SparkSQL.    



![dataflow](https://user-images.githubusercontent.com/35755621/224548693-9cf597be-6c07-4b51-b137-838b0bb1d9e3.png)
