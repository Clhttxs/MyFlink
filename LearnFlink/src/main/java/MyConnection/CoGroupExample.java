package MyConnection;


import akka.stream.impl.ReducerState;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;

public class CoGroupExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> ds1 = env.fromElements(
                Tuple3.of("王五", 23, 1000L),
                Tuple3.of("李四", 24, 2000L),
                Tuple3.of("张三", 25, 3000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((data, l) -> data.f2));
        SingleOutputStreamOperator<Tuple3<String, String,Long>> ds2 = env.fromElements(
                Tuple3.of("赵二", "经理",2000L),
                Tuple3.of("李四", "医生",3000L),
                Tuple3.of("张三", "老师",4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((data, l) -> data.f2));
        CoGroupedStreams.WithWindow<Tuple3<String, Integer,Long>, Tuple3<String, String,Long>, String, TimeWindow> window = ds1.coGroup(ds2)
                .where(r -> r.f0).equalTo(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));
        window.apply(new leftJoin()).print("leftjoin");
        window.apply(new fullJoin()).print("fulljoin");
        env.execute();
    }

    private static class leftJoin implements CoGroupFunction<Tuple3<String, Integer,Long>,Tuple3<String, String,Long>,String> {
        @Override
        public void coGroup(Iterable<Tuple3<String, Integer,Long>> left, Iterable<Tuple3<String, String,Long>> right, Collector<String> collector) throws Exception {
            HashMap<String,String> rightmap = new HashMap<>();
            for(Tuple3<String,String,Long> val:right) rightmap.put(val.f0,val.f1);
            for(Tuple3<String,Integer,Long> val:left){
                collector.collect(val.f0+":"+val.f1+"=>"+rightmap.getOrDefault(val.f0,"没有匹配数据"));
            }
        }
    }

    private static class fullJoin implements CoGroupFunction<Tuple3<String, Integer,Long>,Tuple3<String, String,Long>,String> {

        @Override
        public void coGroup(Iterable<Tuple3<String, Integer, Long>> left, Iterable<Tuple3<String, String, Long>> right, Collector<String> collector) throws Exception {
            //使用 Flink 提供的 MapState 和 BroadcastState 来代替 HashMap，这样可以更好地支持分布式计算。
            HashMap<String,String> rightmap = new HashMap<>();
            HashMap<String,Integer> leftmap = new HashMap<>();
            for(Tuple3<String,String,Long> val:right) rightmap.put(val.f0,val.f1);
            for(Tuple3<String,Integer,Long> val:left) leftmap.put(val.f0, val.f1);
            for(Tuple3<String,Integer,Long> val:left){
                collector.collect(val.f0+":"+val.f1+"=>"+rightmap.getOrDefault(val.f0,"没有匹配数据"));
            }
            for(Tuple3<String,String,Long> val:right){
                if(!leftmap.containsKey(val.f0)) collector.collect("没有匹配数据=>"+val.f0+":"+val.f1);
            }
        }
    }
}