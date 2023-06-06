package Customer;

import MyPOJO.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private boolean running = true;
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Cary","Alex","James","Pure"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1","./prod?id=2","./search","/prod?id=3"};
        while (running){
           sourceContext.collect(new Event(
                   users[random.nextInt(users.length)],
                   urls[random.nextInt(urls.length)],
                   Calendar.getInstance().getTimeInMillis()
                   ));
            Thread.sleep(2000L);
        }
    }

    @Override
    public void cancel() {
        running=false;
    }
}