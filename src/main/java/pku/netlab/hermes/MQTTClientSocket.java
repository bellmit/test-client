package pku.netlab.hermes;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import org.apache.log4j.Logger;
import org.dna.mqtt.moquette.proto.messages.*;
import pku.netlab.hermes.parser.MQTTDecoder;
import pku.netlab.hermes.parser.MQTTEncoder;

import static org.dna.mqtt.moquette.proto.messages.AbstractMessage.*;

/**
 * Base class for connection handling, 1 tcp connection corresponds to 1 instance of this class.
 */
public class MQTTClientSocket implements MQTTPacketTokenizer.MqttTokenizerListener {

    private static Logger logger = Logger.getLogger(MQTTClientSocket.class);


    private MQTTDecoder decoder;
    private MQTTEncoder encoder;
    private MQTTPacketTokenizer tokenizer;
    public NetSocket netSocket;
    private MQTTClient client;
    private String clientID;

    public MQTTClientSocket (NetSocket netSocket, MQTTClient client) {
        this.decoder = new MQTTDecoder();
        this.encoder = new MQTTEncoder();
        this.tokenizer = new MQTTPacketTokenizer();
        this.tokenizer.registerListener(this);
        this.netSocket = netSocket;
        this.client = client;
        this.clientID = client.clientID;
        start();
    }

    private void start() {
        netSocket.setWriteQueueMaxSize(500);
        netSocket.handler(buf -> {
            tokenizer.process(buf.getBytes());
        });
        netSocket.exceptionHandler(event -> {
            String clientInfo = getClientInfo();
            logger.error(clientInfo + ", net-socket exception caught: " + netSocket.writeHandlerID() + " error: " + event.getMessage(), event.getCause());
            clean();
        });
        netSocket.closeHandler(aVoid -> {
            String clientInfo = getClientInfo();
            logger.debug(clientInfo + ", net-socket closed ... " + netSocket.writeHandlerID());
            clean();

        });
    }


    private void sendBytesOverSocket(Buffer bytes) {
        try {
            netSocket.write(bytes);
            if (netSocket.writeQueueFull()) {
                netSocket.pause();
                netSocket.drainHandler(done -> netSocket.resume());
            }
        } catch (Throwable e) {
            logger.error(e.getMessage());
        }
    }

    public void closeConnection() {
        netSocket.close();
    }

    private void clean() {
        if (tokenizer != null) {
            tokenizer.removeAllListeners();
            tokenizer = null;
        }
    }

    @Override
    public void onToken(byte[] token, boolean timeout) throws Exception {
        try {
            if (!timeout) {
                Buffer buffer = Buffer.buffer(token);
                AbstractMessage message = decoder.dec(buffer);
                onMessageFromBroker(message);
            } else {
                logger.debug("Timeout occurred ...");
            }
        } catch (Throwable ex) {
            String clientInfo = getClientInfo();
            logger.error(clientInfo + ", Bad error in processing the message", ex);
            closeConnection();
        }
    }

    @Override
    public void onError(Throwable e) {
        String clientInfo = getClientInfo();
        logger.error(clientInfo + ", " + e.getMessage(), e);
//        if(e instanceof CorruptedFrameException) {
        closeConnection();
//        }
    }

    private void onMessageFromBroker(AbstractMessage msg) throws Exception {
        if (msg.getMessageType() != PUBLISH) {
            logger.debug("Broker >>> " + getClientInfo() + " :" + msg);
        }
        switch (msg.getMessageType()) {
            case CONNACK:
                this.client.onConnect((ConnAckMessage) msg);
                break;
            case SUBSCRIBE:
                break;
            case SUBACK:
                this.client.onSubAck((SubAckMessage)msg);
                break;
            case PUBLISH:
                //ClientManager.counter.update();
                //System.out.println(ClientManager.counter);
                PublishMessage pub = (PublishMessage) msg;
                this.client.onPublish(pub);
                logger.debug("Broker >>> " + getClientInfo() + " :" + pub.getPayloadAsString());
                switch (pub.getQos()) {
                    case LEAST_ONE:
                        PubAckMessage ack = new PubAckMessage();
                        ack.setMessageID(pub.getMessageID());
                        sendMessageToBroker(ack);
                        break;
                    case MOST_ONE:
                        break;
                }
                break;
            case PUBACK:
                this.client.onPubAck((PubAckMessage) msg);
                break;
            case PINGRESP:
                this.client.onPong((PingRespMessage) msg);
                break;
            case PINGREQ:
                PingRespMessage pingResp = new PingRespMessage();
                sendMessageToBroker(pingResp);
                break;
            case DISCONNECT:
                DisconnectMessage disconnectMessage = (DisconnectMessage) msg;
                handleDisconnect(disconnectMessage);
                closeConnection();
                break;
            default:
                logger.debug("type of message not known: " + msg.getClass().getSimpleName());
                break;
        }
    }


    public void sendMessageToBroker(AbstractMessage message) {
        try {
            logger.debug("Broker <<< " + message);
            Buffer b1 = encoder.enc(message);
            sendBytesOverSocket(b1);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void handleDisconnect(DisconnectMessage disconnectMessage) {
    }


    protected String getClientInfo() {
        return this.clientID + "@" + netSocket.localAddress();
    }

}


