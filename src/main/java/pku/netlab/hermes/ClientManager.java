package pku.netlab.hermes;

import io.vertx.core.*;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

import java.io.File;
import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;


/**
 * Created by hult on 11/29/16.
 */
public class ClientManager extends AbstractVerticle {
    private Logger logger = LoggerFactory.getLogger(ClientManager.class);
    private int seq;
    private static int nMsg = 10;
    private static int nBusy = 0;
    private int nClient;
    static String payload;
    ArrayList<MQTTClient> clients;
    private final HashMap<String, Double> map = new HashMap<>();
    private String networkID;
    private Config conf;

    public ClientManager(int i, Config config) {
        this.seq = i;
        this.conf = config;
    }


    @Override
    public void start() throws Exception {
        byte[] bytes = new byte[config().getInteger("payload")];
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
        this.nClient = config.getInteger("nClient") / Main.INSTANCE_NUM;
        this.nBusy = config.getInteger("nBusy") / Main.INSTANCE_NUM;
        this.nMsg = config.getInteger("nMsg");
        this.networkID = config.getString("network");

        this.clients = new ArrayList<>(nClient);
        List<Future> connectFutures = new ArrayList<>(nClient);
        String prefix = String.format("%s_%02d_", this.getIpAddress(networkID), this.seq);

        for (int i = 0; i < nClient; i += 1) {
            MQTTClient client = new MQTTClient(vertx, String.format("%s%06d", prefix, i));
            if (config.containsKey("username") && !config.getString("username").isEmpty()) {
                client.setUsernamePwd(config.getString("username"), config.getString("password"));
            }
            Future connected = client.connectBroker(config.getString("host"), config.getInteger("port"), config.getInteger("idle"));
            connectFutures.add(connected);
            clients.add(client);
        }

        Future<Void> allConnected = Future.future();
        allConnected.setHandler(this::allConnected);

        CompositeFuture.all(connectFutures).setHandler(res -> {
            if (res.succeeded()) {
                System.out.println("ALL CONNECTED");
                allConnected.complete();
            }

        });
    }

    private void allConnected(AsyncResult<Void> voidAsyncResult) {
        batchSubscribe();
        Future<Integer> batchPubFuture = Future.future();
        batchPubFuture.setHandler(res-> {
            if (res.succeeded()) {
                batchPublish(res.result());
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
    }

    private void batchSubscribe() {
        List<Future> allSub = new ArrayList<>(this.nClient);
        CompositeFuture.all(allSub).setHandler(this::allSubscribed);
        for (MQTTClient client : clients) {
            Future subAck = client.subscribe("broadcast", AbstractMessage.QOSType.LEAST_ONE);
            allSub.add(subAck);
        }
    }

    private void allSubscribed(AsyncResult<CompositeFuture> res) {
        System.out.println("all sub acked!");
    }

    private void batchPublish(int QPS) {
        this.nBusy = QPS;
        map.put("start", (double)System.currentTimeMillis());
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
            map.compute("avgRTT", (k, v) -> v == null ? (double)client.statistics.get("total") : v + client.statistics.get("total"));
        }
        map.compute("avgRTT", (k, v) -> v / (nBusy * nMsg));
        map.put("end", (double) System.currentTimeMillis());
        map.put("upstreamQps", (nMsg * nBusy) * 1000.0 / (map.get("end") - map.get("start")));
        System.out.println(map);
        System.exit(0);
    }
}
