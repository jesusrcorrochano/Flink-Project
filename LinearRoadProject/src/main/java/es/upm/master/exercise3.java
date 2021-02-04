package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class exercise3 {
    public static void main(String[] args) throws Exception{
        // get the input arguments
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data from the given input path
        DataStream<String> text = env.readTextFile(params.get("input"));

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // process the input data and filter by the given segment
        SingleOutputStreamOperator<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = text.
                map(new MapFunction<String, Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception{
                        String[] fieldArray = in.split(",");
                        Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8(Long.parseLong(fieldArray[0]),
                                Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[2]), Integer.parseInt(fieldArray[3]), Integer.parseInt(fieldArray[4]),
                                Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]), Integer.parseInt(fieldArray[7]));
                        return out;
                    }
                })
                .filter(new FilterFunction<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public boolean filter(Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                        return in.f6.equals(params.getInt("segment"));
                    }
                });

        // key by the VIDs
        KeyedStream<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0*1000;
                            }
                        }
                ).keyBy(1);

        // apply the window function AverageSpeed within one hour
        SingleOutputStreamOperator<Tuple4<Long,Integer, Integer, Integer>> avgTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(3600))).apply(new exercise3.AverageSpeed());

        // get the correct format for the output 1
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> out1 = avgTumblingEventTimeWindows
                .flatMap(new FlatMapFunction<Tuple4<Long,Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    public void flatMap(Tuple4<Long,Integer, Integer, Integer> in, Collector <Tuple3<Integer, Integer, Integer>> collector) throws Exception {
                        Tuple3<Integer, Integer, Integer> out = new Tuple3(in.f1, in.f2, in.f3);
                        collector.collect(out);
                    }
                });

        // key by xway
        KeyedStream<Tuple4<Long,Integer, Integer, Integer>, Tuple> keyedStream1 = avgTumblingEventTimeWindows.keyBy(2);

        // get the car with the highest average speed in the given segment every hour
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, Integer>> maxTumblingEventTimeWindows =
                keyedStream1.window(TumblingEventTimeWindows.of(Time.seconds(3600))).maxBy(3);

        // get the correct format for the output 2
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> out2 = maxTumblingEventTimeWindows
                .flatMap(new FlatMapFunction<Tuple4<Long,Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    public void flatMap(Tuple4<Long,Integer, Integer, Integer> in, Collector <Tuple3<Integer, Integer, Integer>> collector) throws Exception {
                        Tuple3<Integer, Integer, Integer> out = new Tuple3(in.f1, in.f2, in.f3);
                        collector.collect(out);
                    }
                });

        // emit result
        out1.writeAsCsv(params.get("output1"));
        out2.writeAsCsv(params.get("output2"));

        // execute program
        env.execute("exercise3");
    }

    // window function to calculate the average speed
    public static class AverageSpeed implements WindowFunction<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple4<Long, Integer, Integer, Integer>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple4<Long, Integer, Integer, Integer>> out) throws Exception {
            Iterator<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Long ts=0L;
            Integer VID = 0;
            Integer xway = 0;
            Integer sumSpeed = 0;
            Integer count = 0;
            if(first!=null){
                ts = first.f0;
                VID = first.f1;
                xway = first.f3;
                sumSpeed = first.f2;
                count = 1;
            }
            while(iterator.hasNext()){
                Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                sumSpeed += next.f2;
                count += 1;
            }
            Integer avgSpeed = sumSpeed / count;
            out.collect(new Tuple4<Long, Integer, Integer, Integer>(ts, VID, xway, avgSpeed));
        }

    }

}
