package com.redis.connect.pipeline.event.handler.custom.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.exception.ValidationException;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import io.lettuce.core.RedisConnectionException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Map;


public class SplunkForwardHECRequestStage extends BaseCustomStageHandler {

    private static final String CUSTOM_CONFIG_DESTINATION_URL = "splunk.destination.url";
    private static final String HTTP_HEADERS_KEY = "httpHeaders";
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String destinationUrl;

    public SplunkForwardHECRequestStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
        destinationUrl = jobPipelineStage.getDatabase().getCustomConfiguration().get(CUSTOM_CONFIG_DESTINATION_URL);
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent) throws Exception {

        Map<String, Object> values = changeEvent.getValues();
        if (values != null) {

            if (!values.containsKey(HTTP_HEADERS_KEY))
                throw new ValidationException("Could not forward Splunk HTTP Event Collector (HEC) request because HTTP Headers were not provided");

            HttpHeaders httpHeaders = mapper.readValue((String) values.remove(HTTP_HEADERS_KEY), HttpHeaders.class);
            HttpEntity<String> entity = new HttpEntity<>(mapper.writeValueAsString(values), httpHeaders);

            // Forward HTTP Event Collector Request
            ResponseEntity<String> response = new RestTemplate().postForEntity(URI.create(destinationUrl), entity, String.class);

            if (response == null)
                throw new RedisConnectionException("HTTP request to " + destinationUrl + " with HttpHeaders: " + httpHeaders + " did not receive a response");

            else if (!response.getStatusCode().equals(HttpStatus.OK))
                throw new RedisConnectionException("HTTP request to " + destinationUrl + " with HttpHeaders: " + httpHeaders + " received a response with HttpStatusCode: " + response.getStatusCode());
        }
    }

    @Override
    public void init() throws Exception {
        setSequenceCallback(new Sequence());

        if (destinationUrl == null || destinationUrl.isBlank())
            throw new ValidationException(CUSTOM_CONFIG_DESTINATION_URL + " is a required configuration for this custom stage");
    }

    @Override
    public void shutdown() {
    }

}