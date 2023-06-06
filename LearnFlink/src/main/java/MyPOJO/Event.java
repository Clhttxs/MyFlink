package MyPOJO;

public class Event {
    public String name;
    public String url;
    public long timestamp;

    public Event() {
    }

    public Event(String name, String url, long timestamp) {
        this.name = name;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}