# class-real-time-aggregator

```class-real-time-aggregator``` provides the software for real-time hierarchical temporal aggregation of data coming from smart cameras of the CLASS project. 

In particular, the application must be able to process streams of incoming data in real time, produced by a series of infrastructures contained within the Modena Smart Model Area defined by the MASA (Modena Automotive Smart Area) project.

After storing the data in their raw format in the archiving system, we also want to maintain the flow of aggregated data at various time granularities. To do this, a hierarchical aggregation has been designed, which allows to calculate the aggregates starting from previously obtained results. The temporal granularities chosen are:
* 1 minute
* 15 minutes
* 1 hour
* 1 day

The idea is to calculate the density of the different types of vehicles in each stretch of road covered by the CLASS cameras for each temporal aggregate.

NB: The aggregates are updated in real time thanks to the Spark Streaming framework.

### Features

The project has the objectives of:
* Store the raw stream provided by smart cameras
* Develop an efficient reverse geocoding algorithm, in order to transform the coordinate pairs contained in the stream into pairs (name_road, part_of_the_road)
* Design a hierarchical temporal aggregation of data, in order to have various aggregates for different time windows

### Dependencies
* python 3
* scipy
* pyspark
* Apache Spark (version 2.4.5)

### Use
Run: 
```sh
python script.py
```
to create the directory hierarchy where aggregates will be stored:
* the folder _sink_ keeps aggregate and raw data that Spark doesn't keep in memory
* the folder _checkpoint_ maintains checkpoints for each aggregate
* the folder _stream_ is the directory that accepts the incoming data stream

After that run the following command to launch the spark application:
```sh
spark-submit main.py
```
The next step is to simulate a data stream by inserting txt files (example 15.txt) in the directory named stream

Finally, we can monitor the status and resources used in real time by accessing the Spark Web UI: http://127.0.0.1:4040

### Improvements
Currently, the time aggregated data that Spark does not keep in memory is stored in a "sink_time" type directory (eg sink_one_minute) within the sink folder. This means that if you want to load the temporal aggregates per minute of a single day into memory, you will need to load the entire directory. To avoid this problem in the future it is possible to create, within each directory of the temporal aggregates, a folder for each day.
