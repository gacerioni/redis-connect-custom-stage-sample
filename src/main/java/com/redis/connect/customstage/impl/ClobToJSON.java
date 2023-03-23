package com.redis.connect.customstage.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Map;

public class ClobToJSON extends BaseCustomStageHandler {

    private static final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");
    private final String jsonClobColumnName1;
    private final String jsonClobColumnName2;

    private static final ObjectMapper mapper = new ObjectMapper();
    private final int processors = Runtime.getRuntime().availableProcessors();

    public ClobToJSON(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
        this.jsonClobColumnName1 = System.getenv("REDISCONNECT_CLOB1");
        this.jsonClobColumnName2 = System.getenv("REDISCONNECT_CLOB2");
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent) throws Exception {

        LOGGER.info("Instance: {} -------------------------------------------Stage: CUSTOM", instanceId);

        if (!changeEvent.getValues().isEmpty()) {

            Map<String, Object> values = changeEvent.getValues();

            if (values != null && values.containsKey(jsonClobColumnName1)) {

                JsonNode jsonNode = mapper.readTree((String) values.get(jsonClobColumnName1));

                jsonNode.fieldNames().forEachRemaining(key -> values.put(key, jsonNode.get(key)));

                values.remove(jsonClobColumnName1);
            }
            if (values != null && values.containsKey(jsonClobColumnName2)) {

                JsonNode jsonNode = mapper.readTree((String) values.get(jsonClobColumnName2));

                jsonNode.fieldNames().forEachRemaining(key -> values.put(key, jsonNode.get(key)));

                values.remove(jsonClobColumnName2);
            }
        }
    }

    @Override
    public void init() {
        setSequenceCallback(new Sequence());
        LOGGER.debug("Instance: {} successfully started disruptor (replication pipeline) in ClobToJSON. Available CPU: {}", instanceId, processors);
    }

    @Override
    public void shutdown() {
        LOGGER.debug("Instance: {} successfully shutdown disruptor (replication pipeline) in ClobToJSON. Available CPU: {}", instanceId, processors);
    }

}