package pku.netlab.hermes.pushservice;

import pku.netlab.hermes.QOSUtils;
import pku.netlab.hermes.parser.MQTTEncoder;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

/**
 * Created by kindt on 2016/8/14 0014.
 * 在while循环中，阻塞地从redis的列表中移除第一个消息进行推送
 * 对于QoS >= 1的消息，推送后把消息id放在一个集合中，把消息id和消息体放在哈希表中
 * 收到ack，就从集合中移除这个id，然后从哈希表中移除这个消息体，放到log区
 * 定期清空集合，把集合中的消息都放到推送列表中
 *
 * 这种方法没能实现，现在打算为每一个用户session维护一个队列，队列名是用户id，队列内容是消息id，消息id对应的value是消息内容，对每个用户进行推送
 * 首先解决只对一个用户进行推送的情况，然后再解决广播的情况
 *
 *  qos为0的消息不存储，qos1的消息如同retain消息一样，利用storemanager存储，由应用层去判断是否过期
 */
public class PushVerticle extends AbstractVerticle{
    private EventBus eventbus = null;
    private MQTTEncoder encoder = null;
    private QOSUtils qosUtils = null;
    private RedisClient redisClient = null;

    @Override
    public void start() throws Exception {
        this.encoder = new MQTTEncoder();
        this.eventbus = vertx.eventBus();
        this.qosUtils = new QOSUtils();
        this.redisClient = RedisClient.create(vertx, new RedisOptions().setHost("laptop").setAuth("didi"));
        while (true) {
            pushLoop();
        }
    }

    private void pushLoop() {
        this.redisClient.blpop("pending", 10, ret-> {
            if (ret.failed()) {
                System.out.println(ret.cause().getMessage());
            } else {
                JsonArray array = ret.result();
                if (array != null) {
                    System.out.println(ret.result().toString());
                }
                /*
                if (Objects.nonNull(strmsg)) {
                    JsonObject msg = new JsonObject(strmsg);
                    System.out.println(msg);
                    try {
                        this.eventbus.send(msg.getString("uid"), encoder.enc(PushMessage.convert(msg)));
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    pushLoop();
                } else {
                    return;
                }
                */
            }
        });
    }
}
