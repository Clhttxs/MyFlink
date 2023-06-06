package MyConnection;

import Customer.AppOrder;
import Customer.ThirdOrder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appstream = env.addSource(new AppOrder())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((data, l) -> data.f2));
        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> thirdStream = env.addSource(new ThirdOrder())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((data, l) -> data.f3));
        appstream.join(thirdStream)
                        .where(data1 ->data1.f0).equalTo(data2 -> data2.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                                .apply(new MyJoin())
                                        .print().setParallelism(2);
        env.execute();
    }

    private static class MyJoin implements JoinFunction<Tuple3<String, String, Long>,Tuple4<String,String,String,Long>,String> {
        @Override
        public String join(Tuple3<String, String, Long> left, Tuple4<String, String, String, Long> right) throws Exception {
            return left+"=>"+right;
        }
    }
}