# Dataflow Pipeline Example with Custom Redis Implementation
A custom implementation of HSET for storing data Memorystore in a cloud dataflow project.

## What does it do here?
This is a cloud dataflow pipline code that processes data from a cloud storage bucket, transform it and stores in Google's highly scalable, reduced latency in-memory database called memorystore which is an implementation of Redis (5.0 version)

## What the heck is cloud dataflow?
Well, its a pipeline provided by Google to process data to a form we need and it just takes seconds to transform bulk of data. Companies like Spotify and Dailymotion uses this service.

## Command to execute the pipeline:
```
mvn compile exec:java \
  -Dexec.mainClass=com.click.example.StarterPipeline \
  -Dexec.args="--project=your-project-id \
  --jobName=dataflow-custom-redis-job \
  --stagingLocation=gs://cloud-dataflow-pipeline-bucket/staging/ \
  --dataflowJobFile=gs://cloud-dataflow-pipeline-bucket/templates/dataflow-custom-redis-template \
  --gcpTempLocation=gs://cloud-dataflow-pipeline-bucket/tmp/ \
  --runner=DataflowRunner"
```  

## Check the data inserted in Memorystore (Redis) datastore
For checking whether the processed data is stored in the Redis instance after the dataflow pipeline is executed successfully, you must first connect to the Redis instance from any Compute Engine VM instance located within the same project, region and network as the Redis instance.

1) Create a VM instance and SSH to it

2) Install telnet from apt-get in the VM instance
```
  sudo apt-get install telnet
```
3) From the VM instance, connect to the ip-address of the redis instance
```
  telnet instance-ip-address 6379
```
4) Once you are in the redis, check the keys inserted
```
  keys *
```

## References
https://thedeveloperstory.com/2020/07/24/cloud-dataflow-a-unified-model-for-batch-and-streaming-data-processing/

https://thedeveloperstory.com/2020/07/28/the-technology-behind-spotify-wrapped-2019/

https://redis.io/topics/data-types-intro 

https://beam.apache.org/documentation/programming-guide/
