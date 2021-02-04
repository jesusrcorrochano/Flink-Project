package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
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

public class exercise2 {
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

        // process the input data and filter by eastbound direction (0),
        // the given interval of segments and the cars that exceed the speed limit given
        SingleOutputStreamOperator<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapStream = text
                .map(new MapFunction<String, Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
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
                        return in.f5.equals(0);
                    }
                })
                .filter(new FilterFunction<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public boolean filter(Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                        if (in.f6 >= params.getInt("startSegment") && in.f6 <= params.getInt("endSegment")) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                })
                .filter(new FilterFunction<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    public boolean filter(Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                        if (in.f2 > params.getInt("speed")) {
                            return true;} else {
                            return false;
                        }
                    }
                });

        // key by xway
        KeyedStream<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = mapStream.
                assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public long extractAscendingTimestamp(Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                                return element.f0*1000;
                            }
                        }
                ).keyBy(3);

        // apply the window function SpeedRadar within the time given
        SingleOutputStreamOperator<Tuple4<Long, Integer, Integer, String>> radarTumblingEventTimeWindows =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(params.getLong("time")))).apply(new exercise2.SpeedRadar());

        // emit result
        radarTumblingEventTimeWindows.writeAsCsv(params.get("output"));

        // execute program
        env.execute("exercise2");
    }

    // spots the number of cars that have reported an average speed higher than the given one,
    // and register their VIDs
    public static class SpeedRadar implements WindowFunction<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple4<Long, Integer, Integer, String>, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple4<Long, Integer, Integer, String>> out) throws Exception {
            Iterator<Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Long ts = 0L;
            Integer xway = 0;
            Integer count = 0;
            String VIDs = "[";
            if(first!=null){
                ts = first.f0;
                xway = first.f3;
                count = 1;
                VIDs = VIDs + first.f1;
            }
            while(iterator.hasNext()){
                Tuple8<Long, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                count += 1;
                VIDs = VIDs + " - " + next.f1;
            }
            VIDs = VIDs + "]";
            out.collect(new Tuple4<Long, Integer, Integer, String>(ts, xway, count, VIDs));
        }

    }
}