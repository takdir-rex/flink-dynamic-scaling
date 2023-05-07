/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.join;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.examples.join.WindowJoinSampleData.GradeSource;
import org.apache.flink.streaming.examples.join.WindowJoinSampleData.SalarySource;

import java.io.File;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

/**
 * Example illustrating a windowed stream join between two data streams.
 *
 * <p>The example works on two input streams with pairs (name, grade) and (name, salary)
 * respectively. It joins the steams based on "name" within a configurable window.
 *
 * <p>The example uses a built-in sample data generator that generates the steams of pairs at a
 * configurable rate.
 */
@SuppressWarnings("serial")
public class WindowJoin2 {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        // parse the parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000);
        final long rate = params.getLong("rate", 3L);

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
        System.out.println(
                "To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");

        Configuration conf = new Configuration();
        conf.setInteger("taskmanager.numberOfTaskSlots", 1);
        conf.setInteger("local.number-taskmanager", 20); // for testing more than 1 task manager
        final File checkpointDir = new File(System.getProperty("user.home") + File.separator + "tmp" + File.separator + "checkpoint");
        final File savepointDir = new File(System.getProperty("user.home") + File.separator + "tmp" + File.separator + "savepoint");

        conf.setString(StateBackendOptions.STATE_BACKEND, "filesystem");
        conf.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        conf.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

        // obtain execution environment, run this example in "ingestion time"
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // setting for concurrent partial snapshots
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(0);
        env.getCheckpointConfig()
                .setCheckpointTimeout(
                        60000); // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig()
                .setTolerableCheckpointFailureNumber(0); // shutdown job if failure found
        env.getCheckpointConfig()
                .setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); //has no effect for savepoint only (fix interval checkpoint is not enabled). Change the default in StreamingJobGraphGenerator.getCheckpointingMode(CheckpointConfig checkpointConfig)

        env.disableOperatorChaining();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        //                env.enableCheckpointing(2000);
        //        env.setStateBackend(new HashMapStateBackend());
        //        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // create the data sources for both grades and salaries
        DataStream<Tuple2<String, Integer>> grades =
                GradeSource.getSource(env, rate)
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create())
                        .name("WM grades");

        DataStream<Tuple2<String, Integer>> salaries =
                SalarySource.getSource(env, rate)
                        .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create())
                        .name("WM salaries");

        DataStream<Tuple2<String, Integer>> gradesForwarder = grades.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                if(Calendar.getInstance().get(Calendar.SECOND) == 0){
                    //throw new Exception("Simulate failed");
                }
                return value;
            }
        }).name("FW Grades").setParallelism(1);

        DataStream<Tuple2<String, Integer>> salariesForwarder = salaries.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return value;
            }
        }).name("FW Salaries").setParallelism(1);

        // run the actual window join program
        // for testability, this functionality is in a separate method.
        DataStream<Tuple3<String, Integer, Integer>> joinedStream =
                runWindowJoin(gradesForwarder, salariesForwarder, windowSize);

        ((SingleOutputStreamOperator) joinedStream)
                .uid("join")
                .name("Join")
                .setParallelism(2);

        DataStream<Tuple3<String, Integer, Integer>> joinForwarder = joinedStream.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {

            @Override
            public Tuple3<String, Integer, Integer> map(Tuple3<String, Integer, Integer> value) throws Exception {
                return value;
            }
        }).name("FWJ").setParallelism(1);

//        DataStream<Tuple3<String, Integer, Integer>> joinedStream2 =
//                runWindowJoin(grades, salaries, windowSize);
//
//        ((SingleOutputStreamOperator) joinedStream2).
//                .uid("join1")
//                .name("Join2")
//                .snapshotGroup("snapshot-2");

        // print the results with a single thread, rather than in parallel
        joinForwarder.addSink(new DiscardingSink<>()).setParallelism(1).name("Sink");
//        joinedStream2.addSink(new DiscardingSink<>()).setParallelism(1).uid("Sink2").name("Sink2").snapshotGroup("snapshot-2");

//                System.out.println(env.getExecutionPlan());

        // execute program
        env.execute("Windowed Join Example");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {

        NameKeySelector nameKeySelector = new NameKeySelector();

        return grades.join(salaries)
                .where(nameKeySelector)
                .equalTo(nameKeySelector)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(
                        new JoinFunction<
                                Tuple2<String, Integer>,
                                Tuple2<String, Integer>,
                                Tuple3<String, Integer, Integer>>() {

                            @Override
                            public Tuple3<String, Integer, Integer> join(
                                    Tuple2<String, Integer> first, Tuple2<String, Integer> second) {
                                return new Tuple3<String, Integer, Integer>(
                                        first.f0, first.f1, second.f1);
                            }
                        });
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    /**
     * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
     * In a real use case you should use proper timestamps and an appropriate {@link
     * WatermarkStrategy}.
     */
    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}
