package com.redis.connect.pipeline.event.handler.custom.impl;

import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import com.redis.connect.dto.JobPipelineStageDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Map;

import static com.redis.connect.constants.DomainConstants.*;

public class GabsChangeEventOperationStage extends BaseCustomStageHandler {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");
    private final int processors = Runtime.getRuntime().availableProcessors();

    public GabsChangeEventOperationStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
    }

    @Override
    public void onEvent(ChangeEventDTO changeEvent) throws Exception {

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM", instanceId);

        Map<String, Object> values = changeEvent.getValues();

        if (values != null && values.containsKey("GENREID")) {

            String schemaAndTableName = changeEvent.getSchemaAndTableName();
            String operationType = changeEvent.getOperation();

            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Instance: {} CustomStage::onEvent Processor, schemaAndTableName: {}, operationType: {}", instanceId, schemaAndTableName, operationType);

            // Extract the GENREID value
            Object genreIdObj = values.get("GENREID");

            try {
                int genreId;

                // Handle if it's an Integer, Double, or String
                if (genreIdObj instanceof Integer) {
                    genreId = (Integer) genreIdObj;
                } else if (genreIdObj instanceof Double) {
                    // If it's a double but represents an integer (like 2.0), cast it
                    genreId = ((Double) genreIdObj).intValue();
                } else if (genreIdObj instanceof String) {
                    String genreIdStr = (String) genreIdObj;
                    // Check if the string is a floating point number like "2.0"
                    if (genreIdStr.endsWith(".0")) {
                        genreId = Integer.parseInt(genreIdStr.substring(0, genreIdStr.length() - 2));
                    } else {
                        genreId = Integer.parseInt(genreIdStr);
                    }
                } else {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("Instance: {} - Invalid GENREID type, expected Integer, Double, or String but got: {}", instanceId, genreIdObj.getClass());
                    }
                    return; // Exit early since the type is not valid
                }

                // If GENREID is not equal to 2, change the operation to DELETE
                if (genreId != 2) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Instance: {} - Changing operation to DELETE for GENREID: {}", instanceId, genreId);
                    }
                    // Change the operation to DELETE
                    changeEvent.setOperation(CHANGE_EVENT_DTO_OPERATION_DELETE);
                } else {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Instance: {} - Keeping operation as UPDATE for GENREID: {}", instanceId, genreId);
                    }
                }

            } catch (NumberFormatException e) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Instance: {} - Failed to parse GENREID: {}", instanceId, genreIdObj, e);
                }
            }
        } else {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Instance: {} - GENREID is missing from the change event values.", instanceId);
            }
        }
    }

    @Override
    public void init() throws Exception {
        setSequenceCallback(new Sequence());

        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Instance: {} successfully started disruptor (replication pipeline) in GabsChangeEventOperationStage. Available CPU: {}", instanceId, processors);
    }

    @Override
    public void shutdown() throws Exception {
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Instance: {} successfully shutdown disruptor (replication pipeline) in GabsChangeEventOperationStage. Available CPU: {}", instanceId, processors);
    }

}