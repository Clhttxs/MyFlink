package ProcessFunction;

import Customer.ClickSource;
import MyPOJO.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class ProcessFunctionExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event, l) -> event.timestamp)
                );
        OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
        SingleOutputStreamOperator<String> ds1 = ds.process(new MyProcessFuction(outputTag));
        ds1.print().setParallelism(2);
        ds1.getSideOutput(outputTag).print().setParallelism(2);
        env.execute();
    }

    private static class MyProcessFuction extends ProcessFunction<Event,String> {
        private OutputTag<String> outputTag;

        public MyProcessFuction(OutputTag<String> outputTag) {
            this.outputTag=outputTag;
        }

        @Override
        public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
            if(event.name.equals("Mary")){
                collector.collect("检测到Mary的信息"+event.toString());
            }else if(event.name.equals("Bob")) {
                collector.collect("检测到Bob的信息"+event.toString());
            }else {
                context.output(outputTag, "side-output: " + event.toString());
            }
            System.out.println(context.timerService().currentWatermark());
        }
    }
}