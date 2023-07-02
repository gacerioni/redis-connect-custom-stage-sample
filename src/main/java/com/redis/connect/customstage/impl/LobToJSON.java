package com.redis.connect.customstage.impl;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import java.lang.management.ManagementFactory;
import java.util.Base64;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LobToJSON extends BaseCustomStageHandler {

    private static final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");
    private final String jsonBlobColumnNames;
    private final String jsonClobColumnNames;
    private static final ObjectMapper mapper = new ObjectMapper();
    private final int processors = Runtime.getRuntime().availableProcessors();

    public LobToJSON(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
        this.jsonClobColumnNames = System.getenv("REDISCONNECT_CLOB_COLUMNS");
        this.jsonBlobColumnNames = System.getenv("REDISCONNECT_BLOB_COLUMNS");
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent) throws Exception {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM", instanceId);
        }

        String[] jsonClobColumnNameList = jsonClobColumnNames.split(",", -1);
        String[] jsonBlobColumnNameList = jsonBlobColumnNames.split(",", -1);

        if (changeEvent.getValues() != null && !changeEvent.getValues().isEmpty()) {
            Map<String, Object> values = changeEvent.getValues();
            for (String columnName : jsonClobColumnNameList) {
                if (values.containsKey(columnName)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM, columnName: {}, value: {}", instanceId, columnName, values);
                    }
                    JsonNode jsonNode = mapper.readTree((String) values.get(columnName));
                    jsonNode.fieldNames().forEachRemaining(key -> values.put(key, jsonNode.get(key)));
                    values.remove(columnName);
                }
            }
            for (String columnName : jsonBlobColumnNameList) {
                if (values.containsKey(columnName)) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM, columnName: {}, value: {}", instanceId, columnName, values);
                    }
                    if (isValidJSON((String) values.get(columnName))) {
                        JsonNode jsonNode = mapper.readTree((String) values.get(columnName));
                        jsonNode.fieldNames().forEachRemaining(key -> values.put(key, jsonNode.get(key)));
                        values.remove(columnName);
                    } else {
                        JsonNode jsonNode = mapper.readTree(new String(Base64.getDecoder().decode((String) values.get(columnName))));
                        jsonNode.fieldNames().forEachRemaining(key -> values.put(key, jsonNode.get(key)));
                        values.remove(columnName);
                    }
                }
            }
        }
    }

    public boolean isValidJSON(String json) {
        try {
            mapper.readTree(json);
        } catch (JacksonException e) {
            return false;
        }
        return true;
    }

    @Override
    public void init() {
        setSequenceCallback(new Sequence());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} successfully started disruptor (replication pipeline) in ClobToJSON. Available CPU: {}", instanceId, processors);
        }
    }

    @Override
    public void shutdown() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} successfully shutdown disruptor (replication pipeline) in ClobToJSON. Available CPU: {}", instanceId, processors);
        }
    }

}