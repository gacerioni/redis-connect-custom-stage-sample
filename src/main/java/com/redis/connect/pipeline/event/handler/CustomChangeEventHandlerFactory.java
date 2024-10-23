package com.redis.connect.pipeline.event.handler;

import com.redis.connect.pipeline.event.handler.custom.impl.*;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.exception.ValidationException;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;

public class CustomChangeEventHandlerFactory implements ChangeEventHandlerFactory {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();

    private static final String TYPE_FORWARD_HEC_REQUEST_CUSTOM_STAGE = "FORWARD_HEC_REQUEST";
    private static final String TYPE_TRANSFORM_LOB_TO_JSON_CUSTOM_STAGE = "TRANSFORM_LOB_TO_JSON";
    private static final String TYPE_TRANSFORM_VALUE_TO_UPPER_CASE_STAGE = "TO_UPPER_CASE";
    private static final String TYPE_TRANSFORM_VALUE_TO_DELIMITED_STRING_STAGE = "VALUE_TO_DELIMITED_STRING";
    private static final String TYPE_CALLBACK_HTTP_REQUEST_CUSTOM_STAGE = "CALLBACK_HTTP_REQUEST";
    private static final String TYPE_GEMFIRE_HASH_PREP_STAGE = "GEMFIRE_HASH_PREP_STAGE";
    private static final String TYPE_GEMFIRE_OBJECT_TO_MAP_STAGE = "OBJECT_TO_MAP_STAGE";
    private static final String TYPE_GABS_CHANGE_EVENT_OPERATION_STAGE = "GABS_CHANGE_EVENT_OPERATION_STAGE"; // This is my uber like use case

    private static final Set<String> supportedChangeEventHandlers = new HashSet<>();

    static {
        supportedChangeEventHandlers.add(TYPE_FORWARD_HEC_REQUEST_CUSTOM_STAGE);
        supportedChangeEventHandlers.add(TYPE_TRANSFORM_LOB_TO_JSON_CUSTOM_STAGE);
        supportedChangeEventHandlers.add(TYPE_TRANSFORM_VALUE_TO_UPPER_CASE_STAGE);
        supportedChangeEventHandlers.add(TYPE_TRANSFORM_VALUE_TO_DELIMITED_STRING_STAGE);
        supportedChangeEventHandlers.add(TYPE_CALLBACK_HTTP_REQUEST_CUSTOM_STAGE);
        supportedChangeEventHandlers.add(TYPE_GEMFIRE_HASH_PREP_STAGE);
        supportedChangeEventHandlers.add(TYPE_GEMFIRE_OBJECT_TO_MAP_STAGE);
        supportedChangeEventHandlers.add(TYPE_GABS_CHANGE_EVENT_OPERATION_STAGE); // This is my uber like use case
    }

    @Override
    public ChangeEventHandler getInstance(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) throws Exception {

        ChangeEventHandler changeEventHandler;

        switch (jobPipelineStage.getStageName()) {
            case TYPE_FORWARD_HEC_REQUEST_CUSTOM_STAGE ->
                    changeEventHandler = new SplunkForwardHECRequestStage(jobId, jobType, jobPipelineStage);
            case TYPE_TRANSFORM_LOB_TO_JSON_CUSTOM_STAGE ->
                    changeEventHandler = new TransformLobToJsonStage(jobId, jobType, jobPipelineStage);
            case TYPE_TRANSFORM_VALUE_TO_UPPER_CASE_STAGE ->
                    changeEventHandler = new TransformValueToUpperCaseStage(jobId, jobType, jobPipelineStage);
            case TYPE_TRANSFORM_VALUE_TO_DELIMITED_STRING_STAGE ->
                    changeEventHandler = new TransformValueToDelimitedStringStage(jobId, jobType, jobPipelineStage);
            case TYPE_CALLBACK_HTTP_REQUEST_CUSTOM_STAGE ->
                    changeEventHandler = new CallbackHttpRequestCustomStage(jobId, jobType, jobPipelineStage);
            case TYPE_GEMFIRE_HASH_PREP_STAGE ->
                    changeEventHandler = new GemfireHashSinkPreparationStage(jobId, jobType, jobPipelineStage);
            case TYPE_GEMFIRE_OBJECT_TO_MAP_STAGE ->
                    changeEventHandler = new GemfireObjectToMapPrepStage(jobId, jobType, jobPipelineStage);
            case TYPE_GABS_CHANGE_EVENT_OPERATION_STAGE ->  // This is my uber like use case
                    changeEventHandler = new GabsChangeEventOperationStage(jobId, jobType, jobPipelineStage);
            default -> {
                throw new ValidationException("Instance: " + instanceId + " failed to load change event handler for " +
                        " JobId: " + jobId + " due to an invalid job pipeline Stage: " + jobPipelineStage.getStageName());
            }
        }

        // Called here instead of the BO layer since we need to instantiate the handler in order to have visibility to the validate method
        changeEventHandler.validateEventHandler();

        return changeEventHandler;
    }

    @Override
    public String getType() {
        return "CUSTOM";
    }

    @Override
    public boolean contains(String stageName) {
        return supportedChangeEventHandlers.contains(stageName);
    }

}