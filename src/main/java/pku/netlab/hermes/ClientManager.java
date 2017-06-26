package pku.netlab.hermes;

import io.vertx.core.*;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.commons.io.FileUtils;

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
    private final HashMap<String, Double> map = new HashMap<>();
    private RedisClient redisClient;
    private String networkID;



    @Override
    public void start() throws Exception {
        byte[] bytes = new byte[config().getInteger("payload")];
        this.sleep = config().getInteger("sleepTime");
        for (int i = 0; i < bytes.length; i += 1) bytes[i] = 'a';
        payload = new String(bytes);
        String redisHost = config().getString("redis");
        this.redisClient = RedisClient.create(vertx, new RedisOptions().setHost(redisHost));
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
        this.networkID = config.getString("network");

        this.clients = new ArrayList<>(nClient);
        List<Future> connectFutures = new ArrayList<>(nClient);
        String prefix = this.getIpAddress(networkID) + "_";

        for (int i = 0; i < nClient; i += 1) {
            Future future = Future.future();
            connectFutures.add(future);
            MQTTClient c = new MQTTClient(vertx, String.format("%s%06d", prefix, i));
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
                map.put("maxConnectDelay", (double) maxDelay);
                map.put("avgConnectDelay", ((double)totalDelay) / nClient);
                allConnected.complete();
            }

        });
    }

    private void allConnected(AsyncResult<Void> voidAsyncResult) {
        System.out.println(map);
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

        private String getIpAddress(String networkID) {
        Stream<String> ipStream = Stream.empty();
        try {
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            while (enumeration.hasMoreElements()) {
                NetworkInterface ni = enumeration.nextElement();
                ipStream = Stream.concat(ipStream, ni.getInterfaceAddresses().stream().map(ia -> ia.getAddress().getHostAddress()));
            }
        } catch (SocketException e) {
            logger.error(e.getMessage());
            System.exit(0);
        }
        String ret = ipStream.filter(ip -> ip.startsWith(networkID)).findFirst().get();
        if (ret == null) {
            logger.error("failed to find ip address starts with " + networkID);
            System.exit(0);
        }
        return ret;
    }
}
