package com.redis.connect.pipeline.event.handler.custom.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;

import java.util.Map;

public class GemfireObjectToMapPrepStage extends BaseCustomStageHandler {
    public GemfireObjectToMapPrepStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
    }

    @Override
    public void onEvent(ChangeEventDTO changeEventDTO){
        ObjectMapper mapper = new ObjectMapper();
        changeEventDTO.setValues(mapper.convertValue(changeEventDTO.getValueBlob(), Map.class));
    }

    @Override
    public void init() throws Exception {
        setSequenceCallback(new Sequence());
    }

    @Override
    public void shutdown() throws Exception {

    }
}
