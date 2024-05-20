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

            String col1Key = System.getenv("col1");
            String col2Key = System.getenv("col2");
            String col3Key = System.getenv("col3");

            String col1Value = (String) values.getOrDefault(System.getenv("col1"), "fname");
            String col2Value = (String) values.getOrDefault(System.getenv("col2"), "lname");
            String col3Value = (String) values.getOrDefault(System.getenv("col3"), "hiredate");

            // Update value(s) for col1Value coming from the source to upper case
            if (col1Value != null) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Original " + col1Key + ": " + col1Value);

                values.put(System.getenv("col1"), col1Value.toUpperCase());

                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Updated " + col1Key + ": " + values.get(System.getenv("col1")));
            }

            // Update value(s) for col2Value coming from the source to upper case
            if (col2Value != null) {

                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Original " + col2Key + ": " + col2Value);

                values.put(System.getenv("col2"), col2Value.toUpperCase());

                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Updated " + col2Key + ": " + values.get(System.getenv("col2")));
            }

            // Add col3Value from service call if it's null at the source
            if (col3Value.isBlank() && col3Value.isEmpty()) {

                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Original " + col3Key + ": " + col3Value);

                // Create a value object to hold the URL
                URL url = new URL("http://worldtimeapi.org/api/ip");

                // Open a connection(?) on the URL(?) and cast the response(??)
                urlConnection = (HttpURLConnection) url.openConnection();

                // Now it's "open", we can set the request method, headers etc.
                urlConnection.setRequestMethod("GET");
                urlConnection.setRequestProperty("Accept", "application/json");

                Reader streamReader;

                if (urlConnection.getResponseCode() != 200)
                    streamReader = new InputStreamReader(urlConnection.getErrorStream());
                else
                    streamReader = new InputStreamReader(urlConnection.getInputStream());

                BufferedReader br = new BufferedReader(streamReader);
                String output; String unixtime = "";

                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Output from http://worldtimeapi.org/api/ip API call .... \n");

                // Manually converting the response body InputStream to Map using Jackson
                while ((output = br.readLine()) != null) {
                    Map value = objectMapper.readValue(output, Map.class);

                    if (!value.isEmpty() && value.containsKey("unixtime"))
                        unixtime = Integer.toString((Integer) value.get("unixtime"));
                }

                values.put(System.getenv("col3"), unixtime);

                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Updated " + col3Key + ": " + values.get(System.getenv("col3")));

                br.close();
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