package Customer;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class AppOrder implements SourceFunction<Tuple3<String,String,Long>> {
    private boolean running = true;
    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> sourceContext) throws Exception {
        Random random = new Random();
        int i=0;
        while (running){
            sourceContext.collect(Tuple3.of("order-"+i,"app", Calendar.getInstance().getTimeInMillis()));
            i++;
            Thread.sleep(1000L*random.nextInt(6));
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}