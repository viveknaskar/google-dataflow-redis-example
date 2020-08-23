# Dataflow Pipeline Example with Custom Redis Implementation
A custom implementation of HMSET for storing data Memorystore in a cloud dataflow project. HMSET has been deprecated in Redis 5.0

## What does it do here?
This is a cloud dataflow pipline code that processes data from a cloud storage bucket, transform it and stores in Google's highly scalable, reduced latency in-memory database called memorystore which is an implementation of Redis (5.0 version)

## What the heck is cloud dataflow?
Well, its a pipeline provided by Google to process data to a form we need and it just takes seconds to transform bulk of data. Companies like Spotify and Dailymotion uses this service.

## References
https://thedeveloperstory.com/2020/07/24/cloud-dataflow-a-unified-model-for-batch-and-streaming-data-processing/

https://thedeveloperstory.com/2020/07/28/the-technology-behind-spotify-wrapped-2019/
