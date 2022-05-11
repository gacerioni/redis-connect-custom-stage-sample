package com.redis.connect.customstage;

import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.customstage.impl.CustomStage;
import com.redis.connect.pipeline.event.handler.ChangeEventHandler;
import com.redis.connect.pipeline.event.handler.ChangeEventHandlerFactory;

import java.util.HashSet;
import java.util.Set;

public class CustomChangeEventHandlerFactory implements ChangeEventHandlerFactory {

    private static final String TYPE_CUSTOM_STAGE = "TO_UPPER_CASE";

    private static final Set<String> supportedChangeEventHandlers = new HashSet<>();
    static {
        supportedChangeEventHandlers.add(TYPE_CUSTOM_STAGE);
    }

    @Override
    public ChangeEventHandler getInstance(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) throws Exception  {

        ChangeEventHandler changeEventHandler = null;

        if (TYPE_CUSTOM_STAGE.equals(jobPipelineStage.getStageName())) {
            changeEventHandler = new CustomStage(jobId, jobType, jobPipelineStage);
        }

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