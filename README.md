# sf-crime-statistics-spark-streaming
## step 1
Start up kafka server by docker-swarm.
```
$ docker stack deploy -c=kafka-docker.yml udacity-kafka
```
Execute `kafka_server.py` to produce records to the topic.
```
$ python kafka_server.py
```
Execute `consumer_server.py` to see the consumed records printed in console.
```
python consumer_server.py
```
Below picture is the screenshot of kafka-consumer-console output. 
![kafka-server-console-output.png](./image/kafka-server-console-output.png)

## step 2
Execute `data_stream.py` to see streaming results.
```
$ python data_stream.py
```
The screenshot of the streaming output.
![agg_batch_result.png](./image/agg_batch_result.png)

The screenshot of the progress reporter.
![progress_reporter.png](./image/progress_reporter.png)

Because of the reason that the new spark structured streaming API doesn't support the streaming monitoring. (https://knowledge.udacity.com/questions/158733), So, the spark web UI don't show the streaming tab.
![spark-web-ui.png](./image/spark-web-ui.png)
