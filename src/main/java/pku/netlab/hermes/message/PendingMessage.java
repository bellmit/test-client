package pku.netlab.hermes.message;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hult on 3/8/17.
 */
public class PendingMessage {
    String msg;
    JsonArray targets;

    public PendingMessage(String msg, List<String> targets) {
        this.msg = msg;
        this.targets = new JsonArray();
        for (String t: targets) {
            this.targets.add(t);
        }
    }

    public JsonObject toJson() {
        return new JsonObject().put("msg", msg).put("target", targets);
    }

    @Override
    public String toString() {
        return this.toJson().toString();
    }

    public void PendingMessage(JsonObject obj) {
        this.msg = obj.getString("msg");
        this.targets = obj.getJsonArray("targets");
    }

    public static void main(String[] args) {
        List<String> list = new ArrayList<>(3);
        list.add("alice");
        list.add("bob");
        list.add("cathy");
        PendingMessage message = new PendingMessage("hello", list);
        System.out.println(message);
    }

}
