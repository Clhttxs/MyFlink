package MyConnection;

import Customer.AppOrder;
import Customer.ClickSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {
    public static void main(String[] args) throws Exception {
        long a= 1685684640534L;
        long b = 1685684636524L;
        System.out.println(b-a);
    }
}