package pku.netlab.hermes;

import com.cyngn.kafka.produce.MessageProducer;
import io.vertx.core.*;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CLIException;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hult on 1/10/17.
 */
public class HermesMaster extends AbstractVerticle {
    static ZookeeperClusterManager manager = null;
    static Map<String, Object> status = new HashMap<>();
    static Vertx vertx = null;
    private static Logger logger = LoggerFactory.getLogger(HermesMaster.class);
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        System.out.println("Master verticle initiates");
        NetServer server = vertx.createNetServer(new NetServerOptions().setPort(4321));
        server.connectHandler(sock-> {
            sock.handler(buf-> {
            });
            sock.closeHandler(res -> {
            });
        });
        server.listen(res-> {
        });
    }

        static CommandLine cli(String[] args) {
        CLI cli = CLI.create("java -jar <mqtt-master>-fat.jar")
                .setSummary("A vert.x MQTT Master")
                .addOption(new Option()
                                .setLongName("conf")
                                .setShortName("c")
                                .setDescription("master config file (in json format)")
                                .setRequired(true)
                );

        // parsing
        CommandLine commandLine = null;
        try {
            List<String> userCommandLineArguments = Arrays.asList(args);
            commandLine = cli.parse(userCommandLineArguments);
        } catch(CLIException e) {
            // usage
            StringBuilder builder = new StringBuilder();
            cli.usage(builder);
            System.out.println(builder.toString());
//            throw e;
        }
        return commandLine;
    }

    public static void main(String[] args) {
                CommandLine commandLine = cli(args);
        if(commandLine == null)
            System.exit(-1);

        String confFilePath = commandLine.getOptionValue("c");

        if(confFilePath!=null) {
            try {
                String json = FileUtils.readFileToString(new File(confFilePath), "UTF-8");
                JsonObject config = new JsonObject(json);
                JsonObject zkConfig = config.getJsonObject("zookeepers");
                CuratorFramework curator = CuratorFrameworkFactory.builder()
                        .connectString(zkConfig.getString("zookeeperHosts"))
                        .namespace(zkConfig.getString("rootPath", "io.vertx"))
                        .sessionTimeoutMs(zkConfig.getInteger("sessionTimeout", 20000))
                        .connectionTimeoutMs(zkConfig.getInteger("connectTimeout", 3000))
                        .retryPolicy(new ExponentialBackoffRetry(
                                zkConfig.getJsonObject("retry", new JsonObject()).getInteger("initialSleepTime", 1000),
                                zkConfig.getJsonObject("retry", new JsonObject()).getInteger("maxTimes", 5),
                                zkConfig.getJsonObject("retry", new JsonObject()).getInteger("intervalTimes", 10000))
                        ).build();
                curator.start();
                ClusterManager manager = new ZookeeperClusterManager(curator, "HermesMaster");

                Vertx.clusteredVertx(new VertxOptions().setClusterManager(manager), res -> {
                    if (res.succeeded()) {
                        vertx = res.result();
                        System.out.println("manager id: " + manager.getNodeID());
                        manager.nodeListener(new NodeListener() {
                            @Override
                            public void nodeAdded(String nodeID) {
                                System.out.println(nodeID + " joined");
                            }

                            @Override
                            public void nodeLeft(String nodeID) {
                                System.out.println(nodeID + " left");
                            }
                        });
                        vertx.deployVerticle(HermesMaster.class.getName());
                        vertx.deployVerticle(MessageProducer.class.getName(),
                                new DeploymentOptions().setConfig(config.getJsonObject("kafka")), ret-> {
                                    logger.info("kafka producer initiates");
                                });
                    }
                });
            } catch (IOException e) {
                logger.fatal(e.getMessage(),e);
                System.exit(0);
            }

            }
        }
}

