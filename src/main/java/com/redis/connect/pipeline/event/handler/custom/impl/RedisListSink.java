package com.redis.connect.pipeline.event.handler.custom.impl;

import com.redis.connect.constants.CustomConfigConstants;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.exception.ValidationException;
import com.redis.connect.pipeline.event.handler.impl.BaseEventHandler;
import com.redis.connect.pipeline.event.strategy.domain.impl.DomainStrategy.DomainStrategyBuilder;
import com.redis.connect.pipeline.event.strategy.domain.impl.DomainStrategy.STRATEGY;

import java.util.Map;


public class RedisListSink extends BaseEventHandler {

    public RedisListSink(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) throws Exception {
        super(jobId, jobType, jobPipelineStage, true);

        Map<String, String> customConfiguration = jobPipelineStage.getDatabase().getCustomConfiguration();
        String strategy = customConfiguration.getOrDefault(CustomConfigConstants.REDIS_DOMAIN_STRATEGY, STRATEGY.STRING.name());

        this.setDomainStrategy(new DomainStrategyBuilder(strategy).build());
    }

    @Override
    public void validateEventHandler() throws Exception {

        Map<String, String> customConfiguration = this.getJobPipelineStage().getDatabase().getCustomConfiguration();
        if (customConfiguration.containsKey(CustomConfigConstants.REDIS_DOMAIN_STRATEGY)) {
            String strategy = customConfiguration.get(CustomConfigConstants.REDIS_DOMAIN_STRATEGY);

            if (STRATEGY.MESSAGE_BROKER.name().equals(strategy))
                throw new ValidationException("Domain Strategy: " + strategy + " is not supported by " + this.getJobPipelineStage().getStageName());
        }
    }

    @Override
    public void save(ChangeEventDTO changeEvent, String key) throws Exception {
        String value = this.getDomainStrategy().getValuesAsString(changeEvent, key);
        this.getRedisCommands().lpush(key, value);
    }

    @Override
    public void delete(ChangeEventDTO changeEvent, String key) {
        this.getRedisCommands().unlink(key);
    }

}