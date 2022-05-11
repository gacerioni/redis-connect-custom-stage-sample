package com.redis.connect.pipeline.event.handler.impl;

import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.ChangeEventHandler;
import com.redis.connect.utils.ConnectConstants;

import java.lang.management.ManagementFactory;
import java.util.Map;

/**
 * This is a custom stage Writer that explains END USERS on how to write the code for any custom Stages
 * i.e. not already built with Redis Connect framework. For example, a custom transformation you need
 * to apply before writing the changes to Redis Enterprise target.
 * This is an example with the RDBMS's source in the connector demo's with emp table model.
 * <p>
 * NOTE: Any CustomStage Classes must implement the ChangeEventHandler interface as this is the source of
 * all the changes coming to Redis Connect framework.
 */
public class CustomStage implements ChangeEventHandler<Map<String, Object>> {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();

    protected String jobId;
    protected String jobType;
    protected JobPipelineStageDTO jobPipelineStage;

    public CustomStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.jobPipelineStage = jobPipelineStage;
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent, long sequence, boolean endOfBatch) throws Exception {
        System.out.println("-------------------------------------------Stage: CUSTOM Sequence: " + sequence);

        try {

            if (!changeEvent.isValid())
                System.out.println("Instance: " + instanceId + " received an invalid transition event payload for JobId: " + jobId);

            for (Map<String, Object> payload : changeEvent.getPayloads()) {

                String schemaAndTableName = payload.get(ConnectConstants.CHANGE_EVENT_SCHEMA) + "." + payload.get(ConnectConstants.CHANGE_EVENT_TABLE);

                Map<String, String> values = (Map<String, String>) payload.get(ConnectConstants.CHANGE_EVENT_VALUES);

                String operationType = (String) payload.get(ConnectConstants.CHANGE_EVENT_OPERATION_TYPE);

                System.out.println("CustomStage::onEvent Processor, " + "jobId: " + jobId + ", schemaAndTableName: " + schemaAndTableName + ", operationType: " + operationType);

                if (!values.isEmpty()) {

                    String col1 = values.get("fname");
                    String col2 = values.get("lname");

                    System.out.println("Original col1 value: " + col1);
                    System.out.println("Original col2 value: " + col2);

                    // Update the col1 value(s) coming from the source to upper case
                    values.put("fname", col1.toUpperCase());
                    System.out.println("Updated col1 value: " + values.get("fname"));
                    // Update the col2 value(s) coming from the source to upper case
                    values.put("lname", col2.toUpperCase());
                    System.out.println("Updated col2 value: " + values.get("lname"));
                }
            }
        } catch (Exception e) {
            System.err.println("Instance: " + instanceId + "MESSAGE: " + e.getMessage() + "STACKTRACE: " + e);
        }
    }

}