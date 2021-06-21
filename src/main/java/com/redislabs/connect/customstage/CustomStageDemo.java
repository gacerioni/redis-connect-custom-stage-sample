package com.redislabs.connect.customstage;

import com.redislabs.connect.ConnectConstants;
import com.redislabs.connect.core.BatchEventProducer;
import com.redislabs.connect.core.ChangeEventProducer;
import com.redislabs.connect.core.config.model.HandlerConfig;
import com.redislabs.connect.core.model.ChangeEvent;
import com.redislabs.connect.model.Operation;
import com.redislabs.connect.transport.ChangeEventHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * This is a custom stage Writer that explains END USERS on how to write the code for any custom Stages
 * i.e. not already built with Redis Connect framework.
 * This is an example with the SQL Server connector using a SQL Server Database and emp table model.
 * You can customize the code that suits your use-case.
 * <p>
 * NOTE: Any CustomStage Classes must implement the ChangeEventHandler interface as this is the source of
 * all the changes coming to Redis Connect framework.
 */
@Slf4j
public class CustomStageDemo implements ChangeEventHandler<Map<String, Object>, HandlerConfig> {
    protected HandlerConfig handlerConfig;
    protected String jobId;
    protected String name;

    /**
     *
     *  TO_UPPER_CASE is the unique string which represents the id of the ChangeEventHandler.
     *  It should be mapped in the JobConfig.yml as handlerId.
     *
     * @return String
     */
    @Override
    public String id() {
        return "TO_UPPER_CASE";
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     *  We create object for customStageDemo based on Singleton Design Pattern just to avoid creating the multiple
     *  Instances of Custom Stage
     *
     * @param handlerConfig
     * @return
     * @throws Exception
     */
    @Override
    public ChangeEventHandler getInstance(HandlerConfig handlerConfig) throws Exception {
        CustomStageDemo customStageDemo = new CustomStageDemo();
        customStageDemo.handlerConfig = handlerConfig;
        customStageDemo.jobId = (String)handlerConfig.getConfigurationDetails().get("jobId");
        customStageDemo.name = (String)handlerConfig.getConfigurationDetails().get("name");

        return customStageDemo;
    }

    /**
     * The onEvent Handler is triggered whenever there is change in data i.e.
     * an Insert, Update or Delete event has occurred on the source Database.
     *
     * Called when a publisher has published an event to the {@link ChangeEventProducer}.  The {@link BatchEventProducer} will
     * read messages from the {@link ChangeEventProducer} in batches, where a batch is all of the events available to be
     * processed without having to wait for any new event to arrive.  This can be useful for event handlers that need
     * to do slower operations like I/O as they can group together the data from multiple events into a single
     * operation.  Implementations should ensure that the operation is always performed when endOfBatch is true as
     * the time between that message and the next one is indeterminate.
     *
     * @param changeEvent      published to the {@link ChangeEventProducer}
     * @param sequence   of the event being processed
     * @param endOfBatch flag to indicate if this is the last event in a batch from the {@link ChangeEventProducer}
     * @throws Exception if the EventHandler would like the exception handled further up the chain.
     */
    @Override
    public void onEvent(ChangeEvent<Map<String, Object>> changeEvent, long sequence, boolean endOfBatch) throws Exception {
        if (changeEvent.getPayload() != null && changeEvent.getPayload().get(ConnectConstants.VALUE) != null) {
            Operation op = (Operation) changeEvent.getPayload().get(ConnectConstants.VALUE);
            log.debug("CustomStageDemo::onEvent Processor : {}, table : {}, operation : {}", getJobId(),
                    op.getTable(), op.getType());

            String fname = op.getCols().getCol("fname").getValue();
            String lname = op.getCols().getCol("lname").getValue();
            // Update the fname value(s) coming from sql server source to upper case
            if(fname != null) {
                op.getCols().getCol("fname").setValue(fname.toUpperCase());
            }
            // Update the lname value(s) coming from sql server source to upper case
            if(lname != null) {
                op.getCols().getCol("lname").setValue(lname.toUpperCase());
            }

        }
    }
}
