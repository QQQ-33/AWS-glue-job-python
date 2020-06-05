## This Repository is AWS glue job for pyspark 

The requirement is json data transfom to csv. 

The final data need group by customer, service model name, equipmentId and formate at csv. 
csv file need include some metadata of the equipment at the begining .
So the whole transform process split into two step.
1. er data json to json 
2. er data json to csv 


json to json we use AWS glue job, take data from S3 and RDS.

json to csv we use AWS lambda, trigger by the stepu one file creartion.
