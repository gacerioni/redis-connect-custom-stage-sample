package com.redis.connect.pipeline.event.handler.custom.impl;

import com.lmax.disruptor.Sequence;
import com.redis.connect.dto.ChangeEventDTO;
import com.redis.connect.dto.JobPipelineStageDTO;
import com.redis.connect.pipeline.event.handler.impl.BaseCustomStageHandler;
import java.lang.management.ManagementFactory;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import oracle.sql.TIMESTAMPTZ;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 create table C##RCUSER.TEST(
  ID	            number(8)	not null primary key,
  PIECE1	        varchar2(30),
  PIECE2	        varchar2(30),
  PIECE3	        varchar2(30),
  MODIFIED_DATE		TIMESTAMP(6) WITH TIME ZONE
  );

ALTER TABLE C##RCUSER.TEST ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

Use { "targetColumn": "MODIFIED_DATE", "sourceColumn": "MODIFIED_DATE", "type": "CUSTOM" } mapping for MODIFIED_DATE in the Job configuration
 */
public class TransformValueToDelimitedStringStage extends BaseCustomStageHandler {

    private final String instanceId = ManagementFactory.getRuntimeMXBean().getName();
    private static final Logger LOGGER = LoggerFactory.getLogger("redis-connect");
    private final String environment;

    private final int processors = Runtime.getRuntime().availableProcessors();

    public TransformValueToDelimitedStringStage(String jobId, String jobType, JobPipelineStageDTO jobPipelineStage) {
        super(jobId, jobType, jobPipelineStage);
        this.environment = System.getenv("REDISCONNECT_TEST_ENVIRONMENT");
    }

    @Override
    public void onEvent(ChangeEventDTO<Map<String, Object>> changeEvent) throws Exception {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} -------------------------------------------Stage: CUSTOM", instanceId);
        }

        List<Object> valueAsList = new ArrayList<>();

        if (changeEvent.getValues() != null && !changeEvent.getValues().isEmpty()) {

            Map<String, Object> keyValueAsMap = changeEvent.getValues();
            String schemaAndTableName = changeEvent.getSchemaAndTableName();
            String operationType = changeEvent.getOperation();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Instance: {} TransformValueToDelimitedStringStage::onEvent Processor, schemaAndTableName: {}, operationType: {}, values: {}", instanceId, schemaAndTableName, operationType, keyValueAsMap);
            }

            if ("DEV".equals(environment)) {
                if (schemaAndTableName.equals("C##RCUSER.TEST")) { // We are only interested in the values and not keys as App expects it
                    valueAsList.add(keyValueAsMap.get("ID")); // Adding values to List in the same order as App expects it
                    valueAsList.add(keyValueAsMap.get("PIECE1"));
                    valueAsList.add(keyValueAsMap.get("PIECE2"));
                    valueAsList.add(keyValueAsMap.get("PIECE3"));

                    Object dateObj = keyValueAsMap.get("MODIFIED_DATE");
                    if (dateObj instanceof TIMESTAMPTZ) { // load/snapshot will return oracle.sql.TIMESTAMPTZ
                        final TIMESTAMPTZ ts = (TIMESTAMPTZ) dateObj;
                        dateObj = ts.toZonedDateTime().format(DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss.SSSSSS a x:00"));
                        valueAsList.add(dateObj);
                    } else { // stream job returns date with time zone
                        DateTimeFormatter newFormat = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss.SSSSSS a x:00");
                        valueAsList.add(ZonedDateTime.parse(String.valueOf(dateObj)).format(newFormat));
                    }

                    valueAsList.add("\n");

                    // Final values for this table
                    changeEvent.setValueBlob(String.join("|", StringUtils.join(valueAsList).split(",\\s")).replaceAll("^\\[", "").replaceAll("]$", ""));
                }
            }
        }
    }

    @Override
    public void validateEventHandler() {
        if (environment == null || environment.isEmpty()) {
            LOGGER.error("Instance: {} REDISCONNECT_TEST_ENVIRONMENT environment variable is not set. " +
                    "Please set REDISCONNECT_TEST_ENVIRONMENT variable in redisconnect.conf and provide DEV, PERF or PROD as value", instanceId);
        }
    }

    @Override
    public void init() {
        setSequenceCallback(new Sequence());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} successfully started disruptor (replication pipeline) in TransformValueToDelimitedStringStage. Available CPU: {}", instanceId, processors);
        }
    }

    @Override
    public void shutdown() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Instance: {} successfully shutdown disruptor (replication pipeline) in TransformValueToDelimitedStringStage. Available CPU: {}", instanceId, processors);
        }
    }
}