package ProcessFunction;

import Customer.ClickSource;
import MyPOJO.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SplitStreamByOutputTag {
    // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
    private static OutputTag<Tuple3<String,String,Long>> MaryTag = new OutputTag<Tuple3<String,String,Long>>("Mary-pv"){};
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-pv"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((event, l) -> event.timestamp));
        SingleOutputStreamOperator<Event> ds1 = ds.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, Event>.Context context, Collector<Event> collector) throws Exception {
                if (event.name.equals("Mary"))
                    context.output(MaryTag, Tuple3.of(event.name, event.url, event.timestamp));
                else if (event.name.equals("Bob"))
                    context.output(BobTag, Tuple3.of(event.name, event.url, event.timestamp));
                else collector.collect(event);
            }
        });
        ds1.getSideOutput(MaryTag).print("Mary分流").setParallelism(2);
        ds1.getSideOutput(BobTag).print("Bob分流").setParallelism(2);
        ds1.print("主流").setParallelism(2);
        env.execute();
    }


}