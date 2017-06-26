package pku.netlab.hermes;

import hermes.dataobj.EventGenerator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import org.dna.mqtt.moquette.proto.messages.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

/**
 * Created by hult on 11/28/16.
 */
public class MQTTClient {
    static final int RETRY = 30_000; //in ms
    static final int CONNECT_RETRY = 3;
    private int tried = CONNECT_RETRY;
    private Vertx vertx;
    private NetClient netClient;
    private String host;
    private int port = 1883;
    private int keepAlive = 120; //default ping server every 120s
    protected MQTTClientSocket mqttClientSocket;
    private HashSet<Integer> inFlightMessages;
    public HashMap<Integer, MsgStatus> messagePackage;
    private int globalMsgID = 0;
    protected HashMap<String, Long> statistics;
    private Future<Void> connected;
    private Future<Void> allPublished;
    private int sent = 0;
    private ArrayList<Future> pubFutureList;
    private EventGenerator generator;
    static final String payload = new String(new byte[1024]);
    long rtt = 0;
    private io.vertx.core.shareddata.Counter ackCounter;



    public MQTTClient(Vertx v, String cid) {
        this.vertx = v;
        this.clientID = cid;
        this.messagePackage = new HashMap<>();
        this.inFlightMessages = new HashSet<>();
        this.statistics = new HashMap<>();
        this.generator = new EventGenerator(20);
        vertx.sharedData().getCounter("ack", res-> {
            this.ackCounter = res.result();
        });
    }

    private int getGlobalMsgID() {
        int ret = globalMsgID;
        globalMsgID = (1 + globalMsgID) % 65536;
        return ret;
    }

    public void connectBroker(String host, int port, int keepAlive, Future<Void> future) throws Exception{
        if (host == null || host.length() == 0) throw new Exception("Invalid host.");
        if (port <= 0) throw new Exception("Invalid port number.");
        if (keepAlive < 0) throw new Exception("Keep alive must >= 0.");
        this.host = host;
        this.port = port;
        this.keepAlive = keepAlive;
        this.statistics.put("connectDelay", System.currentTimeMillis());
        this.netClient = vertx.createNetClient(new NetClientOptions()
                .setConnectTimeout(10_000).setReconnectAttempts(10).setReconnectInterval(3_000)
                .setTcpNoDelay(true).setSendBufferSize(2048).setReceiveBufferSize(2048)
                .setUsePooledBuffers(true).setTcpKeepAlive(true));
        this.connected = future;
        rtt = System.currentTimeMillis();
        connectWithRetry(host, port);
    }

    private void connectWithRetry(String host, int port) {
        netClient.connect(port, host, res -> {
            if (!res.succeeded()) {
                logger.warn(String.format("%s tcp connect failed for [%s] and retry", clientID, res.cause().getMessage()));
                connectWithRetry(host, port);
            } else {
                NetSocket sock = res.result();
                mqttClientSocket = new MQTTClientSocket(sock, this);
                logger.info(String.format("TCP from %s at %s established, try mqtt CONNECT", clientID, mqttClientSocket.netSocket.localAddress()));
                sendConnectMsg();
                vertx.setPeriodic(3_000, id -> {
                    if (!connected.isComplete() && tried > 0) {
                        tried -= 1;
                        sendConnectMsg();
                    } else {
                        vertx.cancelTimer(id);
                        tried = CONNECT_RETRY;
                        if (!connected.isComplete()) {
                            logger.warn(String.format("%s at %s not receive connack, tcp reconnect", clientID, mqttClientSocket.netSocket.localAddress()));
                            mqttClientSocket.closeConnection();
                            mqttClientSocket = null;
                            connectWithRetry(host, port);
                        }
                    }
                });
            };
        });
    }


    private void setPingTimer() {
        if (keepAlive > 0) {
            double random = new Random().nextDouble() + keepAlive;
            vertx.setPeriodic((int)(random * 1000), id -> {
                PingReqMessage ping = new PingReqMessage();
                mqttClientSocket.sendMessageToBroker(ping);
            });
        }
    }

    public void onConnect(ConnAckMessage ack) {
        if (connected.isComplete()) {
            logger.warn(String.format("%s receive duplicate connack", clientID));
            return;
        }
        this.subscribe("counter", AbstractMessage.QOSType.LEAST_ONE);
        rtt = System.currentTimeMillis() - rtt;
        logger.info("rtt: " + rtt);
        logger.info(String.format("Broker <=> %s_%s", clientID, mqttClientSocket.netSocket.localAddress()));
        statistics.compute("connectDelay", (k, v)-> System.currentTimeMillis() - v);
        setPingTimer();
        setRetryTimer();
        connected.complete();
    }

    public void onPublish(PublishMessage pub) {
        ackCounter.incrementAndGet(aVoid->{});
    }

    public boolean onPubAck(PubAckMessage ackMessage) {
        int msgID = ackMessage.getMessageID();
        if (!inFlightMessages.remove(msgID)) {
            logger.info(String.format("%s RECEIVE DUP PUBACK %s\n", clientID, msgID));
            return false;
        }
        MsgStatus status = messagePackage.get(msgID);
        status.rtt = System.currentTimeMillis() - status.tCreated;
        logger.info("rtt: " + status.rtt);
        statistics.compute("min", (k, v) -> v == null ? status.rtt : Math.min(v, status.rtt));
        statistics.compute("max", (k, v)-> (v == null? status.rtt: Math.max(v, status.rtt)));
        statistics.compute("n", (k, v)-> (v == null? 1: v + 1));
        statistics.compute("total", (k, v) -> (v == null ? status.rtt : v + status.rtt));
        pubFutureList.get(msgID).complete();
        return true;
    }

    public void pubNMsg(int target, Future<Void> future) {
        this.allPublished = future;
        //这里会溢出，需要控制index
        this.pubFutureList = new ArrayList<>(target);

        for (int i = 0; i < target; i += 1) {
            this.pubFutureList.add(Future.future());
        }
        CompositeFuture.all(pubFutureList).setHandler(f-> {
            if (f.succeeded()) {
                this.allPublished.complete();
                logger.info(clientID + " finished his job");
            } else {
                logger.error(clientID + "failed to finish job for " + f.cause().toString());
            }
        });

        vertx.setPeriodic(1_000, id-> {
            if (this.sent == target) {
                vertx.cancelTimer(id);
            } else {
                this.sent += 1;
                this.publish(clientID, generator.nextEvent().toByteBuffer(), AbstractMessage.QOSType.LEAST_ONE);
                //this.publish(clientID, payload, AbstractMessage.QOSType.LEAST_ONE);
            }
        });
    }

    public double getAvgRtt() {
        return (double) statistics.get("total") / (double) statistics.get("n");
    }


    public void onMessage(PublishMessage msg) {
    }

    public void onPong(PingRespMessage pong) {
    }

    public void onSubAck(SubAckMessage msg) {
        inFlightMessages.remove(msg.getMessageID());
    }

    public void onUnsub(UnsubAckMessage msg) {
        inFlightMessages.remove(msg.getMessageID());
    }


    private void setRetryTimer() {
        vertx.setPeriodic(1000, id-> {
            if (inFlightMessages != null && inFlightMessages.size() > 0) {
                long now = System.currentTimeMillis();
                for (int msgId : inFlightMessages) {
                    if (messagePackage.get(msgId).tLastSent + RETRY < now) {
                        sendMessageWithRetry(messagePackage.get(msgId).msg);
                    }
                }
            }
        });
    }

    public void disconnect() {
        mqttClientSocket.sendMessageToBroker(new DisconnectMessage());
    }

    private void sendConnectMsg() {
        ConnectMessage connectMessage = new ConnectMessage();
        connectMessage.setCleanSession(true);
        connectMessage.setClientID(clientID);
        connectMessage.setKeepAlive(keepAlive);
        connectMessage.setUsername("mobvoi");
        connectMessage.setPassword("mobvoiawesome");
        connectMessage.setPasswordFlag(true);
        connectMessage.setUserFlag(true);
        mqttClientSocket.sendMessageToBroker(connectMessage);
    }

    public void publish(String topic, Object payload, AbstractMessage.QOSType qos)  {
        PublishMessage pub = new PublishMessage();
        pub.setTopicName(topic);
        pub.setQos(qos);
        pub.setMessageType(AbstractMessage.PUBLISH);
        if (payload instanceof String) {
            try {
                pub.setPayload((String)payload);
            } catch (UnsupportedEncodingException e) {
                System.err.println(e.getMessage());
                System.exit(0);
            }
        } else if (payload instanceof ByteBuffer) {
            pub.setPayload((ByteBuffer)payload);
        }
        if (qos == AbstractMessage.QOSType.MOST_ONE) {
            mqttClientSocket.sendMessageToBroker(pub);
        } else {
            pub.setMessageID(getGlobalMsgID());
            sendMessageWithRetry(pub);
        }
    }

    private void sendMessageWithRetry(MessageIDMessage message) {
        int msgID = message.getMessageID();
        mqttClientSocket.sendMessageToBroker(message);
        if (messagePackage.get(msgID) == null) {// if send message the first time
            messagePackage.put(msgID, new MsgStatus(message));
            inFlightMessages.add(msgID);
        } else {
            messagePackage.get(msgID).update();
        }
    }


    public void subscribe(String topic, AbstractMessage.QOSType qosType) {
        SubscribeMessage sub = new SubscribeMessage();
        sub.addSubscription(new SubscribeMessage.Couple(new QOSUtils().toByte(qosType), topic));
        sub.setMessageID(getGlobalMsgID());
        sendMessageWithRetry(sub);
    }


    public static void main(String[] args) throws Exception {
    }

    static class Task {
        int num = 0;
        String topic;
        String[] contents;
        int index = 0;
        public Task(int n, String topic) {
            this.num = n;
            this.topic = topic;
        }

        String nextMessage() {
            if (contents == null) {
                return new String(new byte[1024]);
            }
            return this.contents[index++];
        }
    }

    static class MsgStatus {
        MessageIDMessage msg;
        long tLastSent;
        long tCreated;
        long rtt;
        int retry;
        public MsgStatus(MessageIDMessage msg) {
            this.msg = msg;
            this.retry = 0;
            this.tCreated = System.currentTimeMillis();
            this.tLastSent = tCreated;
        }
        public void update() {
            this.tLastSent = System.currentTimeMillis();
            this.retry += 1;
        }
    }
    private Logger logger = LoggerFactory.getLogger(MQTTClient.class);
    protected String clientID;

}
