package com.redis.connect.pipeline.event.handler.custom.impl;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import java.lang.management.ManagementFactory;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransformLobToJsonStage extends BaseCustomStageHandler {

    private static final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");
    private final String jsonLobColumnNames;
    private static final ObjectMapper mapper = new ObjectMapper();
    private final int processors = Runtime.getRuntime().availableProcessors();

    public TransformLobToJsonStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
        this.jsonLobColumnNames = System.getenv("REDIS_CONNECT_LOB_COLUMNS");
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent) throws Exception {

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM", instanceId);

        Map<String, Object> values = changeEvent.getValues();
        if (values != null && !values.isEmpty()) {

            String[] jsonLobColumnNameList = jsonLobColumnNames.split(",", -1);
            for (String columnName : jsonLobColumnNameList) {

                if (values.containsKey(columnName)) {

                    if (LOGGER.isDebugEnabled()) {
                        System.getenv().forEach((k, v) -> LOGGER.debug(k + ":" + v));
                        LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM, columnName: {}, value: {}", instanceId, columnName, values);
                    }

                    JsonNode jsonNode;
                    try {
                        jsonNode = mapper.readTree(String.valueOf(values.get(columnName)));
                    } catch (JacksonException e) {
                        jsonNode = mapper.readTree(new String(Base64.getDecoder().decode((String) values.get(columnName))));
                    }

                    Iterator<String> iterator = jsonNode.fieldNames();
                    while (iterator.hasNext()) {
                        String fieldName = jsonNode.fieldNames().next();
                        values.put(fieldName, jsonNode.get(fieldName));
                    }
                    values.remove(columnName);
                }
            }
        }
    }

    @Override
    public void validateEventHandler() {
        if (jsonLobColumnNames == null || jsonLobColumnNames.isEmpty()) {
            LOGGER.error("Instance: {} REDISCONNECT_LOB_COLUMNS environment variable is not set. " +
                         "Please set REDISCONNECT_LOB_COLUMNS variable in redisconnect.conf and provide value(s) for one or more comma separated clob/blob column names", instanceId);
        }
    }

    @Override
    public void init() {
        setSequenceCallback(new Sequence());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} successfully started disruptor (replication pipeline) in TransformLobToJsonStage. Available CPU: {}", instanceId, processors);
        }
    }

    @Override
    public void shutdown() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} successfully shutdown disruptor (replication pipeline) in TransformLobToJsonStage. Available CPU: {}", instanceId, processors);
        }
    }

}