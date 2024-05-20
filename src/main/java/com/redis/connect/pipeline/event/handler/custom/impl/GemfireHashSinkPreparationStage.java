package com.redis.connect.pipeline.event.handler.custom.impl;

import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

public class GemfireHashSinkPreparationStage extends BaseCustomStageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");
    public GemfireHashSinkPreparationStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
    }

    @Override
    public void onEvent(ChangeEventDTO changeEvent) throws Exception {
        Map<String,Object> values = ((Map<String, Object>) changeEvent.getValueBlob()).entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e->e.getValue().toString()));
        changeEvent.setValues(values);
    }

    @Override
    public void init() throws Exception {
        setSequenceCallback(new Sequence());
    }

    @Override
    public void shutdown() throws Exception {

    }
}
