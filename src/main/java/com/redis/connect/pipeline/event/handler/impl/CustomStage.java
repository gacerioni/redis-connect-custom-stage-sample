package com.redis.connect.pipeline.event.handler.impl;

import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.dto.JobSourceDTO;
import com.redis.connect.dto.JobSourceTableColumnDTO;
import com.redis.connect.pipeline.event.handler.ChangeEventHandler;
import com.redis.connect.utils.ConnectConstants;

import java.lang.management.ManagementFactory;
import java.util.Map;

public class CustomStage implements ChangeEventHandler<Map<String, Object>> {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();

    protected String jobId;
    protected String jobType;
    protected JobPipelineStageDTO jobPipelineStage;
    protected JobSourceDTO jobSourceDTO;

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

                JobSourceDTO.Table table = jobSourceDTO.getTables().get(schemaAndTableName);

                System.out.println("CustomStage::onEvent Processor, " + "jobId: " + jobId + ", table: " + table + ", operationType: " + operationType);

                if (table != null) {
                    for (JobSourceTableColumnDTO tableColumn : table.getColumns()) {

                        String col1 = String.valueOf(values.get(tableColumn.getSourceColumn()).equalsIgnoreCase((System.getProperty("col1", "fname"))));
                        String col2 = String.valueOf(values.get(tableColumn.getSourceColumn()).equalsIgnoreCase((System.getProperty("col2", "lname"))));

                        System.out.println("Original " + col1 + " : " + col1);
                        System.out.println("Original " + col2 + " : " + col2);

                        // Update the col1 value(s) coming from the source to upper case
                        tableColumn.setTargetColumn(col1.toUpperCase());
                        System.out.println("Updated " + col1 + " : " + tableColumn.getTargetColumn().equals(col1));
                        // Update the col2 value(s) coming from the source to upper case
                        tableColumn.setTargetColumn(col1.toUpperCase());
                        System.out.println("Updated " + col2 + " : " + tableColumn.getTargetColumn().equals(col2));
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Instance: " + instanceId + "MESSAGE: " + e.getMessage() + "STACKTRACE: {} " + e);
        }
    }

}