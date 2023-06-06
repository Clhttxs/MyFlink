package ProcessFunction;

import Customer.ClickSource;
import MyPOJO.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessingTimeTimerTest  {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.timestamp;
                    }
                })
        );
        ds.keyBy(event -> true).process(new MyKeyProcessFunction()).print().setParallelism(2);
        env.execute();
    }

    // 基于 KeyedStream 定义事件时间定时器
    private static class MyKeyProcessFunction extends KeyedProcessFunction<Boolean,Event,String> {

        @Override
        public void processElement(Event event, KeyedProcessFunction<Boolean, Event, String>.Context context, Collector<String> collector) throws Exception {
            collector.collect(event.name+"的数据到达，时间戳为："+event.timestamp);
            collector.collect("水位线为："+context.timerService().currentWatermark()+"\n----------分割线------------------");
            //为每个key注册一个定时器,注册一个 10 秒后的定时器
            //context.timestamp()该时间戳表示事件进入Flink系统的时间。
            context.timerService().registerEventTimeTimer(context.timestamp()+10L);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Boolean, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //onTimer是基于定时器触发的，无法获取到event数据。如果想要得到event数据，就需要把event数据保存在状态中、
            out.collect("定时器触发："+timestamp);
        }
    }



}