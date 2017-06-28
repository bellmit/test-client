package pku.netlab.hermes;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.shareddata.Counter;
import org.apache.log4j.Logger;
import org.dna.mqtt.moquette.proto.messages.*;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

//import io.vertx.core.logging.Logger;

/**
 * Created by hult on 11/28/16.
 */
public class MQTTClient {
    static final int CONNECT_RETRY = 10; //retry times
    private int tried = CONNECT_RETRY;
    private Vertx vertx;
    private NetClient netClient;
    private String username;
    private String password;
    private String host;
    private int port = 1883;
    private int keepAlive = 120; //default ping server every 120s
    protected MQTTClientSocket mqttClientSocket;
    private MQTTSession session;
    private Map<Integer, Future> futures;
    private Future<Void> allPublished;
    private int sent = 0;
    private ArrayList<Future> pubFutureList;
    String payload;
    private Counter ackCounter;


    public MQTTClient(Vertx v, String cid) {
        this.vertx = v;
        this.clientID = cid;
        this.futures = new HashMap<>();
        this.session = new MQTTSession(clientID);
        vertx.sharedData().getCounter("ack", res -> {
            this.ackCounter = res.result();
        });
    }


    public Future connectBroker(Config config) throws Exception {
        this.host = config.randomHost();
        this.port = config.getPort();
        this.keepAlive = config.getInterval();
        this.payload = config.getPayload();
        this.netClient = vertx.createNetClient(new NetClientOptions()
                .setConnectTimeout(10_000).setReconnectAttempts(10).setReconnectInterval(5_000)
                .setTcpNoDelay(true).setSendBufferSize(2048).setReceiveBufferSize(2048)
                .setUsePooledBuffers(true).setTcpKeepAlive(true));
        futures.putIfAbsent(-1, Future.future());
        connectWithRetry(host, port);
        return futures.get(-1);
    }

    private void connectWithRetry(String host, int port) {
        this.vertx.setTimer((long)(5000 * Math.random()), id -> {
            netClient.connect(port, host, res -> {
                if (!res.succeeded()) {
                    logger.debug(String.format("%s tcp connect failed for [%s] and retry", clientID, res.cause().getMessage()));
                    connectWithRetry(host, port);
                } else {
                    NetSocket sock = res.result();
                    mqttClientSocket = new MQTTClientSocket(sock, this);
                    logger.debug(String.format("TCP from %s at %s established, try mqtt CONNECT", clientID, mqttClientSocket.netSocket.localAddress()));
                    sendConnectMsg();
                    vertx.setPeriodic(3_000, login -> {
                        Future connected = futures.get(-1);
                        if (!connected.isComplete() && tried > 0) {
                            tried -= 1;
                            sendConnectMsg();
                        } else {
                            vertx.cancelTimer(login);
                            tried = CONNECT_RETRY;
                            if (!connected.isComplete()) {
                                logger.debug(String.format("%s at %s not receive connect ack, tcp reconnect", clientID, mqttClientSocket.netSocket.localAddress()));
                                mqttClientSocket.closeConnection();
                                mqttClientSocket = null;
                                connectWithRetry(host, port);
                            }
                        }
                    });
                }
            });
        });

    }


    private void setPingTimer() {
        if (keepAlive > 0) {
            double random = new Random().nextDouble() + keepAlive;
            vertx.setPeriodic((int) (random * 1000), id -> {
                PingReqMessage ping = new PingReqMessage();
                mqttClientSocket.sendMessageToBroker(ping);
            });
        }
    }

    public void onConnect(ConnAckMessage ack) {
        Future connected = futures.get(-1);
        if (connected.isComplete()) {
            logger.debug(String.format("%s receive duplicate CONNACK", clientID));
            return;
        }
        connected.complete();
        setPingTimer();
        setRetryTimer();
    }

    public void onPublish(PublishMessage pub) {
        ackCounter.incrementAndGet(aVoid -> {
        });
    }

    public boolean onPubAck(PubAckMessage ackMessage) {
        int msgID = ackMessage.getMessageID();
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
        CompositeFuture.all(pubFutureList).setHandler(f -> {
            if (f.succeeded()) {
                this.allPublished.complete();
                logger.debug(clientID + " finished his job");
            } else {
                logger.error(clientID + "failed to finish job for " + f.cause().toString());
            }
        });

        vertx.setPeriodic(1_000, id -> {
            if (this.sent == target) {
                vertx.cancelTimer(id);
            } else {
                this.sent += 1;
                this.publish(clientID, payload, AbstractMessage.QOSType.LEAST_ONE);
            }
        });
    }


    public void onMessage(PublishMessage msg) {
    }

    public void onPong(PingRespMessage pong) {
    }

    public void onSubAck(SubAckMessage msg) {
        int messageID = msg.getMessageID();
        session.onAck(messageID);
        Future subAck = futures.remove(messageID);
        subAck.complete();
    }

    public void onUnsub(UnsubAckMessage msg) {
        session.onAck(msg.getMessageID());
    }


    private void setRetryTimer() {
        vertx.setPeriodic(1000, id -> {
            for (MessageIDMessage message : session.getUnAckMessages()) {
                sendMessageWithRetry(message);
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
        if (this.username != null) {
            connectMessage.setPasswordFlag(true);
            connectMessage.setUserFlag(true);
            connectMessage.setUsername(this.username);
            connectMessage.setPassword(this.password);
        }
        mqttClientSocket.sendMessageToBroker(connectMessage);
    }

    public void setUsernamePwd(String[] usernamePwd) {
        if (usernamePwd != null) {
            this.username = usernamePwd[0];
            this.password = usernamePwd[1];
        }
    }

    public void publish(String topic, Object payload, AbstractMessage.QOSType qos) {
        PublishMessage pub = new PublishMessage();
        pub.setTopicName(topic);
        pub.setQos(qos);
        pub.setMessageType(AbstractMessage.PUBLISH);
        if (payload instanceof String) {
            try {
                pub.setPayload((String) payload);
            } catch (UnsupportedEncodingException e) {
                System.err.println(e.getMessage());
                System.exit(0);
            }
        } else if (payload instanceof ByteBuffer) {
            pub.setPayload((ByteBuffer) payload);
        }
        if (qos == AbstractMessage.QOSType.MOST_ONE) {
            mqttClientSocket.sendMessageToBroker(pub);
        } else {
            pub.setMessageID(session.getGlobalMsgID());
            sendMessageWithRetry(pub);
        }
    }

    private void sendMessageWithRetry(MessageIDMessage message) {
        mqttClientSocket.sendMessageToBroker(message);
        session.record(message);
    }


    public Future subscribe(String topic, AbstractMessage.QOSType qosType) {
        SubscribeMessage sub = new SubscribeMessage();
        sub.addSubscription(new SubscribeMessage.Couple(new QOSUtils().toByte(qosType), topic));
        int messageID = session.getGlobalMsgID();
        Future subscribed = Future.future();
        futures.put(messageID, subscribed);
        sub.setMessageID(messageID);
        sendMessageWithRetry(sub);
        return subscribed;
    }


    private Logger logger = Logger.getLogger(MQTTClient.class);
    protected String clientID;

}
