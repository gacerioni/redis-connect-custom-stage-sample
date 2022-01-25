package com.redis.connect.customstage;

import com.redis.connect.dto.ChangeEvent;
import com.redis.connect.pipeline.event.handlers.ChangeEventHandler;
import com.redis.connect.pipeline.event.model.Operation;
import com.redis.connect.service.config.model.HandlerConfig;
import com.redis.connect.utils.ConnectConstants;

import java.util.Map;

/**
 * This is a custom stage Writer that explains END USERS on how to write the code for any custom Stages
 * i.e. not already built with Redis Connect framework. For example, a custom transformation you need
 * to apply before writing the changes to Redis Enterprise target.
 * This is an example with the RDBMS's source in the connector demo's with emp/employees table model.
 * Pass any 2 columns with STRING data type to convert them to UPPER CASE
 * e.g. -Dcol1=FIRST_NAME -Dcol2=LAST_NAME
 * <p>
 * NOTE: Any CustomStage Classes must implement the ChangeEventHandler interface as this is the source of
 * all the changes coming to Redis Connect framework.
 */
public class CustomStageDemo implements ChangeEventHandler<Map<String, Object>, HandlerConfig> {

    protected HandlerConfig handlerConfig;
    protected String jobId;
    protected String name;

    /**
     * TO_UPPER_CASE is the unique string which represents the id of the ChangeEventHandler.
     * It should be mapped in the JobConfig.yml as handlerId.
     *
     * @return String
     */
    @Override
    public String id() {
        return "TO_UPPER_CASE";
    }

    /**
     * We create object for CustomStageDemo based on Singleton Design Pattern just to avoid creating the multiple
     * Instances of Custom Stage
     *
     * @param handlerConfig {@link HandlerConfig}
     * @return customStageDemo
     */
    @Override
    public ChangeEventHandler<Map<String, Object>, HandlerConfig> getInstance(HandlerConfig handlerConfig) {
        CustomStageDemo customStageDemo = new CustomStageDemo();
        customStageDemo.handlerConfig = handlerConfig;
        customStageDemo.jobId = (String) handlerConfig.getConfigurationDetails().get(ConnectConstants.CONFIG_JOB_ID);
        customStageDemo.name = (String) handlerConfig.getConfigurationDetails().get("name");

        return customStageDemo;
    }

    /**
     * The onEvent Handler is triggered whenever there is change in data i.e.
     * an Insert, Update or Delete event has occurred on the source Database.
     *
     * @param changeEvent changeEvent
     * @param sequence    of the event being processed
     * @param endOfBatch  flag to indicate if this is the last event in a batch
     */
    @Override
    public void onEvent(ChangeEvent<Map<String, Object>> changeEvent, long sequence, boolean endOfBatch) {

        if (changeEvent.getPayload() != null && changeEvent.getPayload().get(ConnectConstants.VALUE) != null) {

            Operation op = (Operation) changeEvent.getPayload().get(ConnectConstants.VALUE);

            System.out.println("CustomStageDemo::onEvent Processor, " + "jobId: " + jobId +
                    ", table: " + op.getTable() + ", operationType: " + op.getType());

            String col1 = op.getCols().getCol(System.getProperty("col1", "fname")).getValue();
            String col2 = op.getCols().getCol(System.getProperty("col2", "lname")).getValue();

            System.out.println("Original " + col1 + " : " + col1);
            System.out.println("Original " + col2 + " : " + col2);

            // Update the col1 value(s) coming from the source to upper case
            if (col1 != null) {
                op.getCols().getCol(System.getProperty("col1", "fname")).setValue(col1.toUpperCase());
                System.out.println("Updated " + col1 + " : " + col1.toUpperCase());
            }
            // Update the col2 value(s) coming from the source to upper case
            if (col2 != null) {
                op.getCols().getCol(System.getProperty("col2", "lname")).setValue(col2.toUpperCase());
                System.out.println("Updated " + col2 + " : " + col2.toUpperCase());
            }
        }
    }
}
