package pku.netlab.hermes;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * Created by hult on 11/29/16.
 */
public class ClientManager extends AbstractVerticle {
    private Logger logger = Logger.getLogger(ClientManager.class);
    private int seq;
    private int QPS;
    private int duration;
    ArrayList<MQTTClient> clients;
    private final HashMap<String, Double> map = new HashMap<>();
    private Config config;

    public ClientManager(int i, Config config) {
        this.seq = i;
        this.config = config;
        this.QPS = this.config.getQPS();
        this.duration = this.config.getDuration();
        this.clients = new ArrayList<>(this.config.getClientNum());
    }

    @Override
    public void start() throws Exception {
        deployClients();
    }

    private void deployClients() throws Exception {
        List<Future> connectedFutures = new ArrayList<>(config.getClientNum());

        for (int i = 0; i < config.getClientNum(); i += 1) {
            String cid = String.format("%s_%s_%s", config.getIPAddr(), this.seq, i);
            MQTTClient client = new MQTTClient(vertx, cid);
            client.setUsernamePwd(config.getUsernamePwd());
            clients.add(client);
            Future connected = client.connectBroker(config);
            connectedFutures.add(connected);
        }

        CompositeFuture.all(connectedFutures).setHandler(this::allConnected);
    }

    private void allConnected(AsyncResult<CompositeFuture> result) {
        System.out.println("ALL CONNECTED");
        //this.batchSubscribe();
        /*
        Future<Integer> batchPubFuture = Future.future();
        batchPubFuture.setHandler(res-> {
            if (res.succeeded()) {
                System.out.println("ALL PUBLISH DONE");
            }
        });
        DatagramSocket socket = vertx.createDatagramSocket();
        socket.listen(9999, "127.0.0.1", async-> {
            if (async.succeeded()) {
                System.out.println("wait QPS on upd:9999");
                socket.handler(packet-> {
                    String input = new String(packet.data().getBytes(), StandardCharsets.US_ASCII);
                    int QPS = Integer.valueOf(input.endsWith("\n")? input.substring(0, input.length()-1): input);
                    System.out.println("QPS: " + QPS);
                    batchPubFuture.complete(QPS);
                });
            }
        });
        */
    }

    private void batchSubscribe() {
        List<Future> allSub = new ArrayList<>(clients.size());
        CompositeFuture.all(allSub).setHandler(this::allSubscribed);
        for (MQTTClient client : clients) {
            Future subFuture = client.subscribe("broadcast", AbstractMessage.QOSType.LEAST_ONE);
            allSub.add(subFuture);
        }
    }

    private void allSubscribed(AsyncResult<CompositeFuture> res) {
        System.out.println("all sub acked!");
    }

    private void batchPublish(int QPS) {
        /**
         *  some problem here, need rewrite
        map.put("start", (double)System.currentTimeMillis());
        List<Future> allPub = new ArrayList<>(this.QPS);
        for (int i = 0; i < nBusy; i += 1) allPub.add(Future.future());
        CompositeFuture.all(allPub).setHandler(this::allPublished);

        for (int i = 0; i < nBusy; i += 1) {
            clients.get(i).pubNMsg(nMsg, allPub.get(i));
        }
        */
    }

    private void allPublished(AsyncResult<CompositeFuture> compositeFutureAsyncResult) {
        /*
        for (int i = 0; i < nBusy; i += 1) {
            MQTTClient client = clients.get(i);
            long max = client.statistics.get("max");
            map.compute("maxRTT", (k, v) -> v == null ? max : Math.max(v, max));
            map.compute("avgRTT", (k, v) -> v == null ? (double)client.statistics.get("total") : v + client.statistics.get("total"));
        }
        map.compute("avgRTT", (k, v) -> v / (nBusy * nMsg));
        map.put("end", (double) System.currentTimeMillis());
        map.put("upstreamQps", (nMsg * nBusy) * 1000.0 / (map.get("end") - map.get("start")));
        System.out.println(map);
        System.exit(0);
        */
    }

    public static void main(String[] args) throws IOException {
        String config = FileUtils.readFileToString(new File("config.json"), "UTF-8");
        DeploymentOptions options = new DeploymentOptions();
        options.setConfig(new JsonObject(config));
        Vertx.vertx().deployVerticle(ClientManager.class.getName(), options);
    }

}
