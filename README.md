# Custom Stage Demo

Custom Stages in [RedisCDC](https://github.com/RedisLabs-Field-Engineering/RedisCDC) is used when there is a need for custom coding for the purpose of user specific transformations, de-tokenization or any other custom tasks you would want to do before the source data is passed along to the final WRITE stage and persisted in the Redis Enterprise database. RedisCDC is an event driven workflow built using stage driven architecture which create the pipeline for any data flow from a source to Redis Enterprise target. The stages can be built-in stages such as WriteStage and Checkpoint Stage or a Custom Stage. This demo will explain on how to write a very simple custom stage that converts the input source records to an UPPER CASE value and pass it along to the WriteStage.

# Steps to create a Custom Stage
### Prerequisite
```cdc-core``` maven module must be available as a dependency prior to writing the Custom Stage class. Please see the [POM](https://github.com/RedisLabs-Field-Engineering/redis-cdc-custom-stage-demo/blob/master/pom.xml) for an example.

### Step - 1

Create a [Custom Stage Class](https://github.com/RedisLabs-Field-Engineering/redis-cdc-custom-stage-demo/blob/master/src/main/java/com/redislabs/cdc/customstage/CustomStageDemo.java) which implements the ChangeEventHandler interface.

We must override the following Mandatory and Optional methods in order to write the custom stage.
<br>```String id()``` **Mandatory**
<br>```String getJobId()``` **Optional**
<br>```String getName()``` **Optional**
<br>```ChangeEventHandler getInstance(HandlerConfig handlerConfig)``` **Mandatory**
<br>```void onEvent(ChangeEvent<Map<String, Object>> changeEvent, long l, boolean b)``` **Mandatory**

### Step - 2

Copy the Custom Stage Service classpath to [META-INF/services](https://github.com/RedisLabs-Field-Engineering/redis-cdc-custom-stage-demo/blob/master/src/main/resources/META-INF/services/com.ivoyant.cdc.transport.ChangeEventHandler) folder that matches the package name in ChangeEventHandler service configuration.
<br>The Service Loader will pick the Custom Stage during runtime by ChangeEventHandlerProvider.
<br> Build the project and place the output jar in the classpath of redis-cdc job e.g.
<br> Copy the jar directly into the lib folder of the connector OR
<br> Create an ext folder under the lib folder and add the ext folder to the classpath of startup script.

### Step - 3

Create the custom stage configuration in **JobConfig.yml**
<br>All configuration related to stages goes under [JobConfig.yml](https://github.com/RedisLabs-Field-Engineering/RedisCDC/tree/master/connectors/mssql#rediscdc-setup-and-job-management-configurations)
```yaml
stages:
    CustomStage:
      handlerId: TO_UPPER_CASE
```
<br> Add this stage to the existing [RedisCDC SQL Server Connector demo's JobConfig.yml](https://github.com/RedisLabs-Field-Engineering/RedisCDC/tree/master/connectors/mssql/demo)

<br> Continue with the job execution in the demo





  
