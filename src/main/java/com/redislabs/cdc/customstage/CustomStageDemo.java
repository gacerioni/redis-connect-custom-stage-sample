package com.redislabs.cdc.customstage;

import com.ivoyant.cdc.CDCConstants;
import com.ivoyant.cdc.core.config.model.HandlerConfig;
import com.ivoyant.cdc.core.model.ChangeEvent;
import com.ivoyant.cdc.model.Operation;
import com.ivoyant.cdc.transport.ChangeEventHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * This is a custom stage Writer that explains END USERS on how to write the code for any custom Stages
 * i.e. not already built with RedisCDC framework.
 * This is an example with the SQL Server connector using a SQL Server Database and emp table model.
 * You can customize the code that suits your use-case.
 * <p>
 * NOTE: Any CustomStage Classes must implement the ChangeEventHandler interface as this is the source of
 * all the changes coming to RedisCDC framework.
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
     * The onEvent Handler is triggered whenever there is change in data (or)
     * any data is inserted (or) deleted from the Database.
     *
     * @param changeEvent
     * @param l
     * @param b
     */
    @Override
    public void onEvent(ChangeEvent<Map<String, Object>> changeEvent, long l, boolean b) throws Exception {
        if (changeEvent.getPayload() != null && changeEvent.getPayload().get(CDCConstants.VALUE) != null) {
            Operation op = (Operation) changeEvent.getPayload().get(CDCConstants.VALUE);
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
