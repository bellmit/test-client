package pku.netlab.hermes.message;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Created by hult on 3/7/17.
 */
public class Predicates {
    float[][] predicates;
    public Predicates(float[][] limits) throws ArrayStoreException {
        if (limits.length <= 0 || limits[0].length < 2)
            throw new ArrayStoreException();
        this.predicates = limits;
    }

    public JsonObject toJson() {
        JsonObject ret = new JsonObject();
        for (int i = 0; i < predicates.length; i += 1) {
            ret.put(String.valueOf(i), new JsonArray().add(predicates[i][0]).add(predicates[i][1]));
        }
        return ret;
    }

    @Override
    public String toString() {
        return this.toJson().toString();
    }

    public static void main(String[] args) {
        float[][] predicates = new float[5][2];
        for (int i = 0; i < predicates.length; i += 1) {
            predicates[i][0] = (float) (System.currentTimeMillis() % 10) / (float) 1000.0;
            predicates[i][1] = (float) (System.currentTimeMillis() % 10) / (float) 1000.0;
        }
        Predicates p = new Predicates(predicates);
        System.out.println(p);
    }
}
