package com.redis.connect.customstage.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lmax.disruptor.LifecycleAware;
import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.ChangeEventHandler;
import com.redis.connect.utils.ConnectConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This is an example of writing a custom transformation
 * i.e. not already built with Redis Connect framework. For example, a custom transformation you need
 * to apply before writing the changes to Redis Enterprise target.
 * Pass any 2 columns with STRING data type to convert them to UPPER CASE
 * e.g. -Dcol1=fname -Dcol2=lname
 * <p>
 * NOTE: Any CustomStage Classes must implement the ChangeEventHandler interface as this is the source of
 * all the changes coming to Redis Connect framework.
 */
public class ToUpperCase implements ChangeEventHandler<Map<String, Object>>, LifecycleAware {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");

    protected String jobId;
    protected String jobType;
    protected JobPipelineStageDTO jobPipelineStage;

    private HttpURLConnection urlConnection;

    // The LMAX Disruptor uses Sequences as a means to identify each ChangeEventHandler place in its ring buffer.
    private Sequence sequenceCallback;
    ObjectMapper objectMapper = new ObjectMapper();

    private final int processors = Runtime.getRuntime().availableProcessors();

    public ToUpperCase(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.jobPipelineStage = jobPipelineStage;
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent, long sequence, boolean endOfBatch) throws Exception {
        LOGGER.info("Instance: {} -------------------------------------------Stage: CUSTOM Sequence: {} End of Batch: {}", instanceId, sequence, endOfBatch);

        try {

            if (!changeEvent.isValid())
                LOGGER.warn("Instance: {} received an invalid transition event payload for JobId: {}", instanceId, jobId);

            LOGGER.debug("Instance: {} Payload: {}", instanceId, changeEvent.getPayloads());

            for (Map<String, Object> payload : changeEvent.getPayloads()) {

                String schemaAndTableName = String.valueOf(payload.get(ConnectConstants.CHANGE_EVENT_SCHEMA_AND_TABLE));

                Map<String, String> values = (Map<String, String>) payload.get(ConnectConstants.CHANGE_EVENT_VALUES);

                String operationType = (String) payload.get(ConnectConstants.CHANGE_EVENT_OPERATION_TYPE);

                LOGGER.info("Instance: {} CustomStage::onEvent Processor, jobId: {}, schemaAndTableName: {}, operationType: {}", instanceId, jobId, schemaAndTableName, operationType);

                if (!values.isEmpty()) {

                    String col1Key = System.getProperty("col1", "fname");
                    String col2Key = System.getProperty("col2", "lname");
                    String col3Key = System.getProperty("col3", "hiredate");
                    String col1Value = values.getOrDefault(System.getProperty("col1", "fname"), "fname");
                    String col2Value = values.getOrDefault(System.getProperty("col2", "lname"), "lname");
                    String col3Value = values.getOrDefault(System.getProperty("col3", "hiredate"), "hiredate");

                    // Update value(s) for col1Value coming from the source to upper case
                    if (col1Value != null) {
                        LOGGER.debug("Original " + col1Key + ": " + col1Value);
                        values.put(System.getProperty("col1", "fname"), col1Value.toUpperCase());
                        LOGGER.debug("Updated " + col1Key + ": " + values.get(System.getProperty("col1", "fname")));
                    }
                    // Update value(s) for col2Value coming from the source to upper case
                    if (col2Value != null) {
                        LOGGER.debug("Original " + col2Key + ": " + col2Value);
                        values.put(System.getProperty("col2", "lname"), col2Value.toUpperCase());
                        LOGGER.debug("Updated " + col2Key + ": " + values.get(System.getProperty("col2", "lname")));
                    }
                    // Add col3Value from service call if it's null at the source
                    if (col3Value.isBlank() && col3Value.isEmpty()) {
                        LOGGER.debug("Original " + col3Key + ": " + col3Value);

                        // Create a value object to hold the URL
                        URL url = new URL("http://worldtimeapi.org/api/ip");
                        // Open a connection(?) on the URL(?) and cast the response(??)
                        urlConnection = (HttpURLConnection) url.openConnection();
                        // Now it's "open", we can set the request method, headers etc.
                        urlConnection.setRequestMethod("GET");
                        urlConnection.setRequestProperty("Accept", "application/json");

                        Reader streamReader;

                        if (urlConnection.getResponseCode() != 200) {
                            streamReader = new InputStreamReader(urlConnection.getErrorStream());
                        } else {
                            streamReader = new InputStreamReader(urlConnection.getInputStream());
                        }

                        BufferedReader br = new BufferedReader(streamReader);
                        String output;
                        String unixtime = "";
                        LOGGER.debug("Output from http://worldtimeapi.org/api/ip API call .... \n");
                        // Manually converting the response body InputStream to Map using Jackson
                        while ((output = br.readLine()) != null) {
                            Map<String, Object> value = objectMapper.readValue(output, Map.class);
                            if (!value.isEmpty() && value.containsKey("unixtime"))
                                unixtime = Integer.toString((Integer) value.get("unixtime"));
                        }
                        values.put(System.getProperty("col3", "hiredate"), unixtime);
                        LOGGER.debug("Updated " + col3Key + ": " + values.get(System.getProperty("col3", "hiredate")));
                        br.close();
                    }

                    // set operation type to force delete based on a condition e.g. a row marked for deletion
                    //payload.set(ConnectConstants.CHANGE_EVENT_OPERATION_TYPE, ConnectConstants.CHANGE_EVENT_OPERATION_DELETE);
                    //((Map<String, String>) payload.get(ConnectConstants.CHANGE_EVENT_VALUES)).clear();
                }
            }
            /* For a slow event handler update the Sequence Barrier for this ChangeEventHandler
            to notify the BatchEventProcessor that the sequence has progressed. */
            sequenceCallback.set(sequence);
            LOGGER.debug("Instance: {} sequenceCallback sequence: {}", instanceId, sequenceCallback.get());
        } catch (Exception e) {
            LOGGER.error("Instance: " + instanceId + "MESSAGE: " + e.getMessage() + "STACKTRACE: " + e);
        }
    }

    @Override
    public void onStart() {
        LOGGER.debug("Instance: {} successfully started disruptor (replication pipeline) in CustomStage for JobId: {} Available CPU: {}", instanceId, jobId, processors);
    }

    @Override
    public void onShutdown() {
        if (urlConnection != null)
            urlConnection.disconnect();
        LOGGER.debug("Instance: {} successfully shutdown disruptor (replication pipeline) in CustomStage for JobId: {} Available CPU: {}", instanceId, jobId, processors);
    }

    private void sleepRandomly() {
        try {
            Thread.sleep(getMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static long getMillis() {
        return ThreadLocalRandom.current().nextInt(1000, 30000);
    }

    @Override
    public void setSequenceCallback(Sequence sequence) {
        this.sequenceCallback = sequence;
    }
}