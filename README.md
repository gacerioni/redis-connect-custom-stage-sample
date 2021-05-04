# Custom Stage Example
[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

Custom Stage pipelining is used to execute the job in form of various stages. The stages can be WriteStage, Acknowledgement Stage, Checkpoint Stage. This module will explain on how to write any custom stage, and it can be added as pipelining process as part of Job configuration.

JobClaimer does the following:
- Components accessed:  TransportChannel, CheckPointReader, ChangeEventProducer.
- Fetching Job Config and Starting the pipeline processes.

The following is procedure to create any custom stage pipelining processes.

# STEP - 1

Create the custom stage configuration in **JobConfig.yml**
All configuration related to stages goes under JobConfig.yml
```yaml
stages:
  CustomStageDemo:
    handlerId: TO_UPPER_CASE
```

# STEP - 2

The Custom Stage Class must implement ChangeEventHandler interface.

We must override the following methods in order to write the custom stage.
Followings are optional and required methods are to create a custom stage pipeline process.
```String id()``` **Mandatory**
```String getJobId()``` **Optional**
```String getName()``` **Optional**
```ChangeEventHandler getInstance(HandlerConfig handlerConfig)``` **Mandatory**
```void onEvent(ChangeEvent<Map<String, Object>> changeEvent, long l, boolean b)``` **Mandatory**


# STEP - 3

Copy the Custom Stage Service classpath to META-INF/services folder
that matches the package name in ChangeEventHandler service configuration.
The Service Loader will pick the Custom Stage during runtime by ChangeEventHandlerProvider

# CUSTOM STAGE EXAMPLE - PLUGIN JAR

### PREREQUISITES (Important):
**NOTE:
The ```cdc-core``` maven module must be compiled prior to execution of custom stage plugin jar.**

### STEPS:

- Add the following line in ```com.redislabs.cdc.transport.ChangeEventHandler``` file
```
com.redislabs.cdc.customstage.CustomStageDemo
```








  
