package com.redis.connect.pipeline.event.handler;

import com.redis.connect.pipeline.event.handler.custom.impl.TransformValueToDelimitedStringStage;
import com.redis.connect.pipeline.event.handler.custom.impl.TransformValueToUpperCaseStage;
import com.redis.connect.pipeline.event.handler.custom.impl.TransformLobToJsonStage;
import com.redis.connect.pipeline.event.handler.custom.impl.SplunkForwardHECRequestStage;
import com.redis.connect.pipeline.event.handler.custom.impl.RedisListSink;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.exception.ValidationException;
import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;


public class CustomChangeEventHandlerFactory implements ChangeEventHandlerFactory {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();

    private static final String TYPE_FORWARD_HEC_REQUEST_CUSTOM_STAGE = "FORWARD_HEC_REQUEST";
    private static final String TYPE_REDIS_LIST_CUSTOM_SINK = "REDIS_LIST_CUSTOM_SINK";
    private static final String TYPE_TRANSFORM_LOB_TO_JSON_CUSTOM_STAGE = "TRANSFORM_LOB_TO_JSON";
    private static final String TYPE_TRANSFORM_VALUE_TO_UPPER_CASE_STAGE = "TO_UPPER_CASE";
    private static final String TYPE_TRANSFORM_VALUE_TO_DELIMITED_STRING_STAGE = "TransformValueToDelimitedStringStage";

    private static final Set<String> supportedChangeEventHandlers = new HashSet<>();

    static {
        supportedChangeEventHandlers.add(TYPE_FORWARD_HEC_REQUEST_CUSTOM_STAGE);
        supportedChangeEventHandlers.add(TYPE_REDIS_LIST_CUSTOM_SINK);
        supportedChangeEventHandlers.add(TYPE_TRANSFORM_LOB_TO_JSON_CUSTOM_STAGE);
        supportedChangeEventHandlers.add(TYPE_TRANSFORM_VALUE_TO_UPPER_CASE_STAGE);
        supportedChangeEventHandlers.add(TYPE_TRANSFORM_VALUE_TO_DELIMITED_STRING_STAGE);
    }

    @Override
    public ChangeEventHandler getInstance(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) throws Exception {

        ChangeEventHandler changeEventHandler;

        switch (jobPipelineStage.getStageName()) {
            case TYPE_FORWARD_HEC_REQUEST_CUSTOM_STAGE:
                changeEventHandler = new SplunkForwardHECRequestStage(jobId, jobType, jobPipelineStage);
                break;
            case TYPE_REDIS_LIST_CUSTOM_SINK:
                changeEventHandler = new RedisListSink(jobId, jobType, jobPipelineStage);
                break;
            case TYPE_TRANSFORM_LOB_TO_JSON_CUSTOM_STAGE:
                changeEventHandler = new TransformLobToJsonStage(jobId, jobType, jobPipelineStage);
                break;
            case TYPE_TRANSFORM_VALUE_TO_UPPER_CASE_STAGE:
                changeEventHandler = new TransformValueToUpperCaseStage(jobId, jobType, jobPipelineStage);
                break;
            case TYPE_TRANSFORM_VALUE_TO_DELIMITED_STRING_STAGE:
                changeEventHandler = new TransformValueToDelimitedStringStage(jobId, jobType, jobPipelineStage);
                break;
            default: {
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