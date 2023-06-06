package Customer;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ThirdOrder implements SourceFunction<Tuple4<String,String,String,Long>> {
    private boolean running = true;
    @Override
    public void run(SourceContext<Tuple4<String, String,String, Long>> sourceContext) throws Exception {
        Random random = new Random();
        String[] state = new String[]{"success","fail"} ;
        int i=0;
        while (running){
            sourceContext.collect(Tuple4.of("order-"+i,"third",state[0], Calendar.getInstance().getTimeInMillis()));
            i++;
            Thread.sleep(1000L*random.nextInt(6));
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}