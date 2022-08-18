package com.redis.connect.customstage.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.ChangeEventHandler;
import com.redis.connect.utils.ConnectConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Map;


public class ClobToJSON implements ChangeEventHandler<Map<String, Object>> {

    private static final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");

    private String jobId;
    private String jobType;
    private JobPipelineStageDTO jobPipelineStage;

    private Sequence sequenceCallback;
    private String jsonClobColumnName;

    private static final ObjectMapper mapper = new ObjectMapper();

    public ClobToJSON(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.jobPipelineStage = jobPipelineStage;
        this.jsonClobColumnName = "TRANS_MSG"; //This can be optionally configured through an ENV variable
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent, long sequence, boolean endOfBatch) throws Exception {

        if (!changeEvent.isValid()) {
            LOGGER.error("Instance: {} JobId: {} received an invalid change event in StageName: {} which will be ignored. " +
                    "If this does not resolve after a few iterations, manual analysis is recommended", instanceId, jobId, jobPipelineStage.getStageName());
            return;
        }

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Instance: {} processed event Stage: {} with Sequence: {} EndOfBatch: {} for JobId: {}", instanceId, jobPipelineStage.getStageName(), sequence, endOfBatch, jobId);

        for (Map<String, Object> payload : changeEvent.getPayloads()) {

            if (payload != null) {

                Map<String, Object> values = (Map<String, Object>) payload.get(ConnectConstants.CHANGE_EVENT_VALUES);
                if (values != null && values.containsKey(jsonClobColumnName)) {

                    JsonNode jsonNode = mapper.readTree((String) values.get(jsonClobColumnName));

                    jsonNode.fieldNames().forEachRemaining(key -> {
                        values.put(key, jsonNode.get(key));
                    });

                    values.remove(jsonClobColumnName);
                }
            }
        }

        /* For a slow event handler update the Sequence Barrier for this ChangeEventHandler to notify the BatchEventProcessor that the sequence has progressed. */
        sequenceCallback.set(sequence);
    }

    @Override
    public void setSequenceCallback(Sequence sequence) {
        this.sequenceCallback = sequence;
    }

}