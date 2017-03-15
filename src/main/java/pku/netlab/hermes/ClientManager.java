package pku.netlab.hermes;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

;

/**
 * Created by hult on 11/29/16.
 */
public class ClientManager extends AbstractVerticle {
    private Logger logger = LoggerFactory.getLogger(ClientManager.class);
    private static int nMsg = 10;
    private static int nBusy = 0;
    private static long start = 0;
    private static int sleep;
    static String payload;
    static Counter counter = new Counter();
    ArrayList<MQTTClient> clients;
    private final HashMap<String, Long> map = new HashMap<>();

    @Override
    public void start() throws Exception {
        byte[] bytes = new byte[config().getInteger("payload")];
        this.sleep = config().getInteger("sleepTime");
        for (int i = 0; i < bytes.length; i += 1) bytes[i] = 'a';
        payload = new String(bytes);
        deployNClients(config());
    }

    public static void main(String[] args) throws IOException {
        String config = FileUtils.readFileToString(new File("config.json"), "UTF-8");
        DeploymentOptions options = new DeploymentOptions();
        options.setConfig(new JsonObject(config));
        Vertx.vertx().deployVerticle(ClientManager.class.getName(), options);
    }

    private void deployNClients(JsonObject config) throws Exception {
        System.out.println(config);
        int nClient = config.getInteger("nClient");
        this.nBusy = config.getInteger("nBusy");
        this.nMsg = config.getInteger("nMsg");

        this.clients = new ArrayList<>(nClient);
        List<Future> connectFutures = new ArrayList<>(nClient);

        for (int i = 0; i < nClient; i += 1) {
            Future future = Future.future();
            connectFutures.add(future);
            MQTTClient c = new MQTTClient(vertx, String.format("%s%06d", "CC", i));
            c.connectBroker(config.getString("host"), config.getInteger("port"), config.getInteger("idle"), future);
            clients.add(c);
        }

        Future<Void> allConnected = Future.future();
        allConnected.setHandler(this::allConnected);

        CompositeFuture.all(connectFutures).setHandler(res -> {
            if (res.succeeded()) {
                Long totalDelay = 0L, maxDelay = 0L;
                for (MQTTClient client : clients) {
                    Long delay = client.statistics.get("connectDelay");
                    totalDelay += delay;
                    maxDelay = Math.max(maxDelay, delay);
                }
                map.put("maxConnectDelay", maxDelay);
                map.put("avgConnectDelay", totalDelay / nClient);
                allConnected.complete();
            }

        });
    }

    private void allConnected(AsyncResult<Void> voidAsyncResult) {
        System.out.println(map);
        if (nMsg > 0) {
            vertx.setTimer(this.sleep * 1_000, id-> {
                batchPublish();
            });
        }
    }

    private void batchPublish() {
        map.put("start", System.currentTimeMillis());
        List<Future> allPub = new ArrayList<>(nBusy);
        for (int i = 0; i < nBusy; i += 1) allPub.add(Future.future());
        CompositeFuture.all(allPub).setHandler(this::allPublished);

        for (int i = 0; i < nBusy; i += 1) {
            clients.get(i).pubNMsg(nMsg, allPub.get(i));
        }

    }

    private void allPublished(AsyncResult<CompositeFuture> compositeFutureAsyncResult) {
        for (int i = 0; i < nBusy; i += 1) {
            MQTTClient client = clients.get(i);
            long max = client.statistics.get("max");
            map.compute("maxRTT", (k, v) -> v == null ? max : Math.max(v, max));
            map.compute("avgRTT", (k, v) -> v == null ? client.statistics.get("total") : v + client.statistics.get("total"));
        }
        map.compute("avgRTT", (k, v) -> v / (nBusy * nMsg));
        map.put("end", System.currentTimeMillis());
        map.put("upstreamQps", (nMsg * nBusy) * 1000 / (map.get("end") - map.get("start")));
        System.out.println(map);
        //System.exit(0);
    }
}
