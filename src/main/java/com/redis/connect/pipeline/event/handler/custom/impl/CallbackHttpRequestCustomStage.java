package com.redis.connect.pipeline.event.handler.custom.impl;

import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.exception.ValidationException;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import io.lettuce.core.RedisConnectionException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class CallbackHttpRequestCustomStage extends BaseCustomStageHandler {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");
    private static final String CUSTOM_CONFIG_DESTINATION_URL = "callback.url";
    private final String destinationUrl;

    public CallbackHttpRequestCustomStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
        destinationUrl = jobPipelineStage.getDatabase().getCustomConfiguration().get(CUSTOM_CONFIG_DESTINATION_URL);
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent) {

        Map<String, Object> values = changeEvent.getValues();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM, destinationUrl: {}, Raw values: {}", instanceId, destinationUrl, values);
        }

        if (values != null && values.containsKey("clientId")) {

            String clientId = String.valueOf(values.remove("clientId"));

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Instance: {} CallbackHttpRequestCustomStage::onEvent Processor, clientId: {}, values: {}", instanceId, clientId, values);
            }

            // Compose GET Request
            ResponseEntity<String> response = new RestTemplate().getForEntity(destinationUrl + "/" + clientId, String.class);

            if (response.getBody() == null)
                throw new RedisConnectionException("HTTP request to " + destinationUrl + " did not receive a response");

            else if (!response.getStatusCode().equals(HttpStatus.OK))
                throw new RedisConnectionException("HTTP request to " + destinationUrl + " received a response with HttpStatusCode: " + response.getStatusCode());

            values.put(clientId, response.getBody());
        }
    }

    @Override
    public void init() throws Exception {
        setSequenceCallback(new Sequence());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} successfully started disruptor (replication pipeline) in CallbackHttpRequestCustomStage.", instanceId);
        }

        if (destinationUrl == null || destinationUrl.isBlank())
            throw new ValidationException(CUSTOM_CONFIG_DESTINATION_URL + " is a required configuration for this custom stage");
    }

    @Override
    public void shutdown() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} successfully shutdown disruptor (replication pipeline) in CallbackHttpRequestCustomStage.", instanceId);
        }
    }

}