package MyConnection;

import Customer.AppOrder;
import Customer.ThirdOrder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.addSource(new AppOrder())
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((data, l) -> data.f2));
        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> thirdStream = env.addSource(new ThirdOrder())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((data, l) -> data.f3));
        // 检测同一支付单在两条流中是否匹配，不匹配就报警
        appStream.connect(thirdStream)
                .keyBy(data1->data1.f0,data2-> data2.f0)
                        .process(new OrderMatchResult())
                                .print().setParallelism(2);
        env.execute();
    }

    // 自定义实现 CoProcessFunction
    private static class OrderMatchResult extends CoProcessFunction<Tuple3<String,String,Long>, Tuple4<String,String,String,Long>,String> {
        // 定义状态变量，用来保存已经到达的事件,类似spark双流join中的redis的作用。
        private ValueState<Tuple3<String,String,Long>> appOrderState;
        private ValueState<Tuple4<String,String,String,Long>> thirdOrderState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appOrderState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("app-Order", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG)));
            thirdOrderState =getRuntimeContext().getState(new ValueStateDescriptor<Tuple4<String, String, String, Long>>("third-Order",Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.LONG)));
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> appValue, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            //先查看另外一个流里面是否有对应的订单，并检查订单支付是否成功
            if(thirdOrderState.value()!=null&&thirdOrderState.value().f2.equals("success")){
                collector.collect("app对账成功："+appValue+" "+thirdOrderState.value()+" 水位线信息："+context.timerService().currentWatermark());
                //清空状态
                thirdOrderState.clear();
            }else {
                //更新状态列表，注册定时器(每个订单的状态只保留五秒，然后使用定时器删除)
                appOrderState.update(appValue);
                context.timerService().registerEventTimeTimer(appValue.f2+5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> thirdValue, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context context, Collector<String> collector) throws Exception {
            //这个流的处理方式与app流类似
            if(appOrderState.value()!=null&&thirdValue.f2.equals("success")){
                collector.collect("第三方对账成功："+appOrderState.value()+" "+thirdValue+" 水位线信息："+context.timerService().currentWatermark());
                appOrderState.clear();
            }else {
                thirdOrderState.update(thirdValue);
                context.timerService().registerEventTimeTimer(thirdValue.f3+5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //触发定时器，删除已经存在5秒但是还没有配对成功的状态数据
            //一个定时器对应一个key，一个状态也是对应一个key =>因此一个定时器对应一个状态
            if(appOrderState.value()!=null&&thirdOrderState.value()!=null){
                out.collect("对账失败："+appOrderState.value()+" "+thirdOrderState.value()+"第三方平台支付失败"+" 水位线信息："+ctx.timerService().currentWatermark());
            }
            else if(appOrderState.value()!=null){
                out.collect("对账失败："+appOrderState.value()+"第三方平台支付超时"+" 水位线信息："+ctx.timerService().currentWatermark());
            } else if (thirdOrderState.value()!=null) {
                out.collect("对账失败："+thirdOrderState.value()+"app平台支付超时"+" 水位线信息："+ctx.timerService().currentWatermark());
            }
            appOrderState.clear();
            thirdOrderState.clear();
        }
    }


}