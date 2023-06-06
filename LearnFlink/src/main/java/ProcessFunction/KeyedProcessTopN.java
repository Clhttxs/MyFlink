package ProcessFunction;

import Customer.ClickSource;
import MyPOJO.Event;
import MyPOJO.UrlViewCount;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KeyedProcessTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(((event, l) -> event.timestamp))
        );
        SingleOutputStreamOperator<UrlViewCount>  urlCountStream = eventStream.keyBy(event -> event.url).window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlViewCountAgg(), new UrlViewCountResult());
        eventStream.print("输入数据").setParallelism(2);
        // 对结果中同一个窗口的统计数据，进行排序处理
        urlCountStream.keyBy(data -> data.windowEnd).process(new MyTopNByKeyProcessFunction(3)).print("TopN").setParallelism(2);
        env.execute();
    }

    // 自定义增量聚合
    private static class UrlViewCountAgg implements AggregateFunction<Event,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    // 自定义全窗口函数，只需要包装窗口信息
    private static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount,String, TimeWindow> {
        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> collector) throws Exception {
            // 结合窗口信息，包装输出内容
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            collector.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
        }
    }

    //自定义处理函数，排序取 top n
    private static class MyTopNByKeyProcessFunction extends KeyedProcessFunction<Long,UrlViewCount,String> {
        private Integer n=2;
        // 定义一个列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        public MyTopNByKeyProcessFunction() {
        }

        public MyTopNByKeyProcessFunction(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从环境中获取列表状态句柄
            urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("url-view-count-list", Types.POJO(UrlViewCount.class)));
        }

        @Override
        public void processElement(UrlViewCount urlViewCount, KeyedProcessFunction<Long, UrlViewCount, String>.Context context, Collector<String> collector) throws Exception {
            //把每个窗口中包装好的urlViewCount放入状态列表
            urlViewCountListState.add(urlViewCount);
            //注册 window end + 1ms 后的定时器，等待所有数据到齐开始排序
            //getCurrentKey()方法返回的是Long类型的key
            context.timerService().registerEventTimeTimer(context.getCurrentKey()+1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将数据从列表状态变量中取出，放入 ArrayList，方便排序
            List<UrlViewCount> list = new ArrayList<>();
            Iterable<UrlViewCount> urlViewCounts = urlViewCountListState.get();
            for(UrlViewCount urlViewCount:urlViewCounts) list.add(urlViewCount);
            // 清空状态，释放资源
            urlViewCountListState.clear();
            Collections.sort(list,(a,b)->{
               return b.count.intValue()-a.count.intValue();
            });
            // 取前n名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            result.append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            for (int i = 0; i < this.n; i++) {
                UrlViewCount UrlViewCount = list.get(i);
                String info = "No." + (i + 1) + " "
                        + "url：" + UrlViewCount.url + " "
                        + "浏览量：" + UrlViewCount.count + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            out.collect(result.toString());
        }
    }
}