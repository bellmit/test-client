package pku.netlab.hermes;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.dna.mqtt.moquette.proto.messages.MessageIDMessage;

import java.util.*;

/**
 * Created by hult on 6/27/17.
 */
public class MQTTSession {
    private Map<Integer, MsgStatus> inFlightMessages;
    static final int RETRY = 10_000; //in ms
    String clientID;

    public MQTTSession(String clientID) {
        this.clientID = clientID;
        this.inFlightMessages = new HashMap<>();
    }

    public List<MessageIDMessage> getUnAckMessages() {
        List<MessageIDMessage> ret = new ArrayList<>(inFlightMessages.size());
        if (inFlightMessages.size() > 0) {
            long now = System.currentTimeMillis();
            for (MsgStatus status: inFlightMessages.values()) {
                if (status.tLastSent + RETRY < now) {
                    ret.add(status.msg);
                }
            }
        }
        return ret;
    }

    public boolean onAck(int msgID) {
        if (!inFlightMessages.containsKey(msgID)) {
            logger.debug(String.format("%s RECEIVE DUP PUBACK %s\n", clientID, msgID));
            return false;
        }
        inFlightMessages.remove(msgID);
        return true;
    }

    public int getGlobalMsgID() {
        int ret = globalMsgID;
        globalMsgID = (1 + globalMsgID) % 65536;
        return ret;
    }

    public void record(MessageIDMessage message) {
        int msgID = message.getMessageID();
        if (!inFlightMessages.containsKey(msgID)) {// if send message the first time
            inFlightMessages.put(msgID, new MsgStatus(message));
        } else {
            inFlightMessages.get(msgID).update();
        }
    }

    private int globalMsgID = 0;
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

    private Logger logger = LoggerFactory.getLogger(MQTTSession.class);

 }
