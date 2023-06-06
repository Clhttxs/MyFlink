package ProcessFunction;

import Customer.ClickSource;
import MyPOJO.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

public class ProcessAllWindowTopN {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> ds = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((event, l) -> event.timestamp)
                );
        ds.print().setParallelism(2);
        // 只需要 url 就可以统计数量，所以转换成 String 直接开窗统计
        ds.map(event -> event.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new TopNProcessAllFunction())
                .print().setParallelism(2);
        env.execute();
    }


    private static class TopNProcessAllFunction extends ProcessAllWindowFunction<String,String,TimeWindow>{
        //这里创建hashmap，相当于对整个流进行统计，而不是针对每个窗口
        //HashMap<String,Long> map = new HashMap<>();
        @Override
        public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
            HashMap<String,Long> map = new HashMap<>();
            Iterator<String> urls = iterable.iterator();
            //使用Hashmap计算出每个url的访问量
            while (urls.hasNext()){
                String url = urls.next();
                map.put(url,map.getOrDefault(url,0L)+1L);
            }
            //将map中的数据放到list中
            List<Tuple2<String, Long>> mapList = new ArrayList<Tuple2<String, Long>>();
            Set<String> keys = map.keySet();
            for(String url :keys) mapList.add(Tuple2.of(url,map.get(url)));
            //对list进行排序，取前N个数据
            Collections.sort(mapList,(a,b)->{
                return (int)(b.f1 - a.f1);
            });
            // 取排序后的前两名，构建输出结果
            StringBuilder result = new StringBuilder();

            result.append("========================================\n");
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> temp = mapList.get(i);
                String info = "浏览量 No." + (i + 1) +
                        " url：" + temp.f0 +
                        " 浏览量：" + temp.f1 +
                        " 窗 口 结 束 时 间 ： " + new Timestamp(context.window().getEnd()) + "\n";
                result.append(info);
            }
            result.append("========================================\n");
            collector.collect(result.toString());
        }
    }
}