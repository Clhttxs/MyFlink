package WordCount;

import Customer.ClickSource;
import MyPOJO.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.time.Duration;

public class CustomSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> ds = env.addSource(new ClickSource());
        /*SingleOutputStreamOperator<Event> ds1 = ds.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
                // 向自定义 WatermarkOutput 输出水位线

        );
        ds1.print("test").setParallelism(4);*/
        SingleOutputStreamOperator<Event> ds1 = ds.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
        );
        ds1.print("输入的数据");
        ds1.keyBy(event -> event.url).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(10))).aggregate(new MyUrlCount()).print("计算结果");
        env.execute();
    }

    private static class MyUrlCount implements AggregateFunction<Event, Tuple2<String,Long>,String> {

        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of(null,0L);
        }

        @Override
        public Tuple2<String, Long> add(Event event, Tuple2<String, Long> acc) {
            return Tuple2.of(event.url,acc.f1+1L);
        }

        @Override
        public String getResult(Tuple2<String, Long> acc) {
            return "Url:"+acc.f0+"数量为:"+acc.f1;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> acc, Tuple2<String, Long> acc1) {
            return null;
        }
    }
}