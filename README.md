# Custom Stage Demo

Custom Stages in Redis Connect is used when there is a need for custom coding for the purpose of user specific transformations, de-tokenization or any other custom tasks you would want to do before the source data is passed along to the final WRITE stage and persisted in the Redis Enterprise database. Redis Connect is an event driven workflow built using stage driven architecture which create the pipeline for any data flow from a source to Redis Enterprise target. The stages can be built-in stages such as `REDIS_HASH_SINK` or a Custom Stage first e.g. `TO_UPPER_CASE` then apply the final write stage e.g. `REDIS_HASH_SINK`. This demo will explain on how to write a very simple custom transformation that converts the input source records to an UPPER CASE value and pass it along to the final write stage.

# Steps to create a Custom Stage

### Prerequisite
```connect-core``` maven module must be available as a dependency prior to writing the Custom Stage class. Please see the [POM](pom.xml) for an example.

### Step - 1

Create a [CustomStage](src/main/java/com/redis/connect/customstage/impl/CustomStage.java) class which implements the ChangeEventHandler interface.

We must override the following methods in order to write the custom stage.
* ```onEvent(ChangeEventDTO<Map<String, Object>> changeEvent, long sequence, boolean endOfBatch)```
* ```setSequenceCallback(Sequence sequence)```
### Step - 2

Create a [CustomChangeEventHandlerFactory](src/main/java/com/redis/connect/customstage/CustomChangeEventHandlerFactory.java) class which implements the ChangeEventHandlerFactory interface and copy this custom factory class to [META-INF/services](src/main/resources/META-INF/services/com.redis.connect.pipeline.event.handler.ChangeEventHandlerFactory) folder that matches the package name in ChangeEventHandlerFactory service configuration.
<br>The Service Loader will pick up this custom factory class during runtime by ChangeEventHandlerFactory.

We must instantiate the CustomStage class within the getInstance() method.
* ```changeEventHandler = new CustomStage(jobId, jobType, jobPipelineStage);```

### Step - 3

Build the project and place the output jar into the extlib folder of Redis Connect. See an example [here](https://github.com/redis-field-engineering/redis-connect-dist/tree/main/examples/postgres/demo/extlib)

### Step - 4

Create the custom stage configuration in the job payload e.g. [cdc-custom-job.json](https://github.com/redis-field-engineering/redis-connect-dist/blob/main/examples/postgres/demo/config/samples/payloads/cdc-custom-job.json)
<br>For example,
```json
{
  "index": 1,
  "stageName": "TO_UPPER_CASE",
  "userDefinedType": "CUSTOM"
}
```

<br>After Redis Connect job (`loader` or `cdc`) execution, you should see that the value of [col1](https://github.com/redis-field-engineering/redis-connect-custom-stage-demo/blob/main/src/main/java/com/redis/connect/customstage/impl/CustomStage.java#L74) and [col2](https://github.com/redis-field-engineering/redis-connect-custom-stage-demo/blob/main/src/main/java/com/redis/connect/customstage/impl/CustomStage.java#L75) in Redis has been transformed to UPPER CASE values.

## Troubleshooting using [Java Debug Wire Protocol (JDWP)](https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/introclientissues005.html)

- Start Redis Connect after supplying `jdwp` as JVM options
    * *[nixOS](https://en.wikipedia.org/wiki/NixOS)
      * Edit `~/redis-connect/bin/redisconnect.conf` and update `REDISCONNECT_JAVA_OPTIONS` 
      ```bash
      REDISCONNECT_JAVA_OPTIONS="-XX:+HeapDumpOnOutOfMemoryError -Xms1g -Xmx2g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
      ```
  * Windows OS
      * Edit `redis-connect\bin\redisconnect.cmd` and update `REDISCONNECT_JAVA_OPTIONS`
    ```cmd
    set REDISCONNECT_JAVA_OPTIONS=-XX:+HeapDumpOnOutOfMemoryError -Xms1g -Xmx2g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005
    ```
  * Docker
      * Pass `REDISCONNECT_JAVA_OPTIONS` as environment variable
    ```bash
    -e REDISCONNECT_JAVA_OPTIONS="-Xms1g -Xmx2g -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
    ```

- Start Custom Stage client code in `DEBUG` mode with same `HOST` and `PORT` then, add a breakpoint. See an example [DEBUG configuration](.run/DEBUG%20CustomStage.run.xml)
