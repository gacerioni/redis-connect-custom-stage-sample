# Custom Stage Demo

Custom Stages in [Redis Connect](https://github.com/redis-field-engineering/redis-connect-dist) is used when there is a need for custom coding for the purpose of user specific transformations, de-tokenization or any other custom tasks you would want to do before the source data is passed along to the final WRITE stage and persisted in the Redis Enterprise database. Redis Connect is an event driven workflow built using stage driven architecture which create the pipeline for any data flow from a source to Redis Enterprise target. The stages can be built-in stages such as WriteStage and Checkpoint Stage or a Custom Stage. This demo will explain on how to write a very simple custom stage that converts the input source records to an UPPER CASE value and pass it along to the WriteStage.

# Steps to create a Custom Stage
### Prerequisite
```connect-core``` maven module must be available as a dependency prior to writing the Custom Stage class. Please see the [POM](https://github.com/redis-field-engineering/redis-connect-custom-stage-demo/blob/main/pom.xml) for an example.

### Step - 1

Create a [Custom Stage Class](https://github.com/redis-field-engineering/redis-connect-custom-stage-demo/blob/main/src/main/java/com/redis/connect/customstage/CustomStageDemo.java) which implements the ChangeEventHandler interface.

We must override the following Mandatory and Optional methods in order to write the custom stage.
<br>```String id()``` **Mandatory**
<br>```String getJobId()``` **Optional**
<br>```String getName()``` **Optional**
<br>```ChangeEventHandler getInstance(HandlerConfig handlerConfig)``` **Mandatory**
<br>```void onEvent(ChangeEvent<Map<String, Object>> changeEvent, long l, boolean b)``` **Mandatory**

### Step - 2

Copy the Custom Stage Service classpath to [META-INF/services](https://github.com/redis-field-engineering/redis-connect-custom-stage-demo/blob/main/src/main/resources/META-INF/services/com.redislabs.connect.transport.ChangeEventHandler) folder that matches the package name in ChangeEventHandler service configuration.
<br>The Service Loader will pick the Custom Stage during runtime by ChangeEventHandlerProvider.
<br>Build the project and place the output jar into the extlib folder of the connector. See an example [here](https://github.com/redis-field-engineering/redis-connect-dist/tree/main/connectors/postgres/demo/extlib)

### Step - 3

Create the custom stage configuration in **JobConfig.yml**
<br>For example, add the handlerId of this Custom Stage under `stages` in the `JobConfig.yml` with a `key` i.e. `CustomStage` (this could be any name of your choice) then add the `key handlerId` and it's value `TO_UPPER_CASE`
<br>All configuration related to stages goes under [JobConfig.yml](https://github.com/redis-field-engineering/redis-connect-dist/blob/main/connectors/postgres/demo/config/samples/postgres/JobConfig.yml)
```yaml
stages:
    CustomStage:
      handlerId: TO_UPPER_CASE
```
<br>Please see an example [here](https://github.com/redis-field-engineering/redis-connect-dist/tree/main/connectors/postgres/demo#custom-stage)
<br>Continue with the job execution in the [demo](https://github.com/redis-field-engineering/redis-connect-dist/tree/main/connectors/postgres/demo#custom-stage)
<br>After the Redis Connect job (`loader` or `cdc`) execution, you should see that the value of [fname](https://github.com/redis-field-engineering/redis-connect-custom-stage-demo/blob/main/src/main/java/com/redislabs/connect/customstage/CustomStageDemo.java#L96) and [lname](https://github.com/redis-field-engineering/redis-connect-custom-stage-demo/blob/main/src/main/java/com/redislabs/connect/customstage/CustomStageDemo.java#L100) columns in Redis has been transformed to UPPER CASE.
