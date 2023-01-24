package org.apache.flink.streaming.examples.join;

import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;

public class FailingSink<T> implements SinkFunction<T> {

    private static final Logger LOG = LoggerFactory.getLogger(FailingSink.class);

    private int count = 0;

    @Override
    public void invoke(T value, Context context) throws Exception {
//        if(Calendar.getInstance().get(Calendar.SECOND) < 10 && count < 20){
//            LOG.info(value.toString());
//            count++;
//        }
        LOG.info(value.toString());
//        if(Calendar.getInstance().get(Calendar.SECOND) == 0){
//            throw new Exception("Simulate failed");
//        }
    }
}
