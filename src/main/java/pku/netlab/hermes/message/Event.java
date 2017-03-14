package pku.netlab.hermes.message;

import io.vertx.core.json.JsonObject;

/**
 * Created by hult on 3/7/17.
 */
public class Event {
    float[] values;
    public Event(float[] values) throws ArrayStoreException{
        if (values == null || values.length <= 0) throw new ArrayStoreException();
        this.values = values;
    }

    public JsonObject toJson() {
        JsonObject ret = new JsonObject();
        for (int i =0; i < values.length; i += 1) {
            ret.put(String.valueOf(i), values[i]);
        }
        return ret;
    }

    @Override
    public String toString() {
        return this.toJson().toString();
    }

    public static void main(String[] args) {
        JsonObject object = new JsonObject().put("1", 1.2).put("0", 3.2);
        System.out.println(Event.toEvent(object));
    }

    public static Event randomEvent() {
        float[] values = new float[20];
        for (int i = 0; i < values.length; i += 1) values[i] = (float) Math.random();
        return new Event(values);
    }

    static Event toEvent(String strEvent) {
        JsonObject ret = new JsonObject(strEvent);
        return Event.toEvent(ret);
    }

    static Event toEvent(JsonObject object) {
        float[] values = new float[object.size()];
        for (int i = 0; i < object.size(); i += 1) {
            values[i] = object.getFloat(String.valueOf(i));
        }
        return new Event(values);
    }


}
