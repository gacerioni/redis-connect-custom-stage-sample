package com.redis.connect.pipeline.event.handler.custom.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an example of writing a custom transformation
 * i.e. not already built with Redis Connect framework. For example, a custom transformation you need
 * to apply before writing the changes to Redis Enterprise target.
 * Pass any 2 columns with STRING data type to convert them to UPPER CASE
 * e.g. From cli do, export col1=fname and export col2=lname
 * OR add it to redisconnect.conf as col1=fname and col2=lname
 * OR with container use -e col1=fname, -e col2=lname
 * <p>
 * NOTE: Any CustomStage Classes must extend the BaseCustomStageHandler class as this is the source of
 * all the changes coming to Redis Connect Custom Stage framework.
 */
public class TransformValueToUpperCaseStage extends BaseCustomStageHandler {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");

    private HttpURLConnection urlConnection;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final int processors = Runtime.getRuntime().availableProcessors();

    public TransformValueToUpperCaseStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
    }

    @Override
    public void onEvent(ChangeEventDTO changeEvent) throws Exception {

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM", instanceId);

        Map<String, Object> values = changeEvent.getValues();
        if (!values.isEmpty()) {

            String schemaAndTableName = changeEvent.getSchemaAndTableName();
            String operationType = changeEvent.getOperation();

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Instance: {} CustomStage::onEvent Processor, schemaAndTableName: {}, operationType: {}", instanceId, schemaAndTableName, operationType);

            // Extract the "NAME" column value (specific to CHINOOK TRACK table)
            String trackName = (String) values.get("NAME");

            // If the "NAME" column exists, convert it to uppercase
            if (trackName != null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Original NAME: {}", trackName);
                }

                // Convert the "NAME" value to uppercase
                values.put("NAME", trackName.toUpperCase());

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Updated NAME: {}", values.get("NAME"));
                }
            }
        }
    }

    @Override
    public void validateEventHandler() {
        // Do validations here
    }

    @Override
    public void init() {
        setSequenceCallback(new Sequence());

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Instance: {} successfully started disruptor (replication pipeline) in TransformValueToUpperCaseStage. Available CPU: {}", instanceId, processors);
    }

    @Override
    public void shutdown() {
        if (urlConnection != null)
            urlConnection.disconnect();

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Instance: {} successfully shutdown disruptor (replication pipeline) in TransformValueToUpperCaseStage. Available CPU: {}", instanceId, processors);
    }
}