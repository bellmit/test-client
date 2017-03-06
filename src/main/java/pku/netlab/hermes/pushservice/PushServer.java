package pku.netlab.hermes.pushservice;

import pku.netlab.hermes.QOSUtils;
import pku.netlab.hermes.parser.MQTTEncoder;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.redis.RedisClient;

/**
 * Created by kindt on 2016/7/21 0021.
 */
public class PushServer extends AbstractVerticle {
    private EventBus eventBus = null;
    private MQTTEncoder encoder = null;
    private QOSUtils qosUtils = null;
    private MongoClient mongoClient = null;
    public static RedisClient redisClient = null;
    @Override
    public void start() throws Exception {
        this.encoder = new MQTTEncoder();
        this.eventBus = vertx.eventBus();
        this.qosUtils = new QOSUtils();
        //config.put("host", "laptop").put("port", 27017).put("username", "didi").put("password", "f").put("authSource", "locations").put("db_name", "locations");
        this.mongoClient = DBClient.getMongoClient();
        this.redisClient = DBClient.getRedisClient();

        System.out.println("push api runnint in " + Thread.currentThread().getName());

        HttpServer server = vertx.createHttpServer();
        server.requestHandler(request-> {
            if (request.path().contains("pushservice") && request.method() == HttpMethod.POST) {
                Buffer totalBuffer = Buffer.buffer();
                request.handler(buffer-> {
                    totalBuffer.appendBuffer(buffer);
                });
                request.endHandler(v-> {
                    String strMsg = totalBuffer.toString();
                    JsonObject jsonMsg = new JsonObject(strMsg);
                    //点对点推送
                    if (request.path().contains("pointtopoint")) {
                        // 把消息写入数据库，获得全局消息ID
                        this.mongoClient.insert("messages", jsonMsg, ret-> {
                            if (ret.succeeded()) {
                                String msgID = ret.result();
                                this.pushMessage(msgID, jsonMsg);
                                request.response().end("message write to database, id " + msgID);
                            } else {
                                System.out.println("failed to process message" + ret.cause().getMessage());
                                request.response().end("failed to process message [" + ret.cause().getMessage() + "]" );
                            }
                        });
                    }else if (request.path().contains("broadcast")) {
                        //广播
                        broadcast(jsonMsg);
                    }
                });
            } else if (request.path().endsWith("/loc") && request.method() == HttpMethod.POST) {
                //发布地理位置，测试用
                Buffer totalBuffer = Buffer.buffer();
                request.handler(buffer-> {
                    totalBuffer.appendBuffer(buffer);
                });
                request.endHandler(v-> {
                    JsonObject jsonObject = new JsonObject(totalBuffer.toString());
                    mongoClient.save("loc", jsonObject, res-> {
                        if (res.succeeded()) {
                            System.out.println(res.result());
                            request.response().end("location write to db");
                        } else {
                            System.out.println(res.cause().getMessage());
                            request.response().end("failed to write to db");
                        }
                    });
                });

            } else {
                request.response().end("wrong path");
            }
        });
        server.listen(8080, result-> {
            if (result.succeeded()) {
                System.out.println("push api starts");
            } else {
                System.out.println("push server failed to deployManyVerticles because of " + result.cause().getMessage());
            }
        });
    }

    private void pushMessage(String msgID, JsonObject jsonItem)  {
        //每个用户都订阅了自己uid作为topic，这里可以交给mqtt的业务逻辑进行发布
        String uid = jsonItem.getString("uid");
        this.redisClient.hget("user:" + uid, "mseq", hgetret -> {
            int mseq = 0;
            if (hgetret.result() != null) {
                mseq = Integer.valueOf(hgetret.result()) + 1 % 65536;
            }

            this.redisClient.hset("user:" + uid, "mseq", String.valueOf(mseq), null);
            jsonItem.put("msgID", mseq);

            Future<Long> addFuture = Future.future();
            Future<Void> setFuture = Future.future();
            /*
            首先把消息seq放入用户的待推送集合中，如mq:alex
            然后把消息seq和全局消息id做一次映射，key为alex:1024，值为全局消息id
            这样用户就可以根据用户的ack中的seq来移除全局消息id
             */
            this.redisClient.sadd("mq:"+ uid , String.valueOf(mseq), addFuture.completer());
            this.redisClient.set(uid + ":" + mseq, msgID, setFuture.completer());

            CompositeFuture.all(addFuture, setFuture).setHandler(ret-> {
                try {
                    //通过eventBus发布消息
                    eventBus.send(uid, encoder.enc(PushMessage.convert(jsonItem)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
    }

    private void broadcast(JsonObject jsonMsg) {
        JsonArray uids = jsonMsg.getJsonArray("uid");
        try {
            for (int i = 0; i < uids.size(); i += 1) {
                String uid = uids.getString(i);
                jsonMsg.put("uid", uid);
                eventBus.send(uid, encoder.enc(PushMessage.convert(jsonMsg)));
            }
        }catch (Exception e) {
            e.printStackTrace();
        }

    }


}
