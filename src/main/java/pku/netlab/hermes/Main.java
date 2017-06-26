package pku.netlab.hermes;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CLIException;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.FileUtils;
import pku.netlab.hermes.tracer.Tracer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

;

/**
 * Created by Giovanni Baleani on 13/11/2015.
 */
public class Main {

    private static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        start(args);
    }

    static CommandLine cli(String[] args) {
        CLI cli = CLI.create("java -jar <mqtt-broker>-fat.jar")
                .setSummary("A vert.x MQTT Broker")
                .addOption(new Option()
                        .setLongName("conf")
                        .setShortName("c")
                        .setDescription("vert.x config file (in json format)")
                        .setRequired(true)
                );
        CommandLine commandLine = null;
        try {
            List<String> userCommandLineArguments = Arrays.asList(args);
            commandLine = cli.parse(userCommandLineArguments);
        } catch (CLIException e) {
            StringBuilder builder = new StringBuilder();
            cli.usage(builder);
            System.out.println(builder.toString());
        }
        return commandLine;
    }

    public static void start(String[] args) {
        System.out.println("start");
        CommandLine commandLine = cli(args);
        if (commandLine == null) {
            System.out.println("no config file");
            System.exit(-1);
        }

        String confFilePath = commandLine.getOptionValue("c");

        DeploymentOptions deploymentOptions = new DeploymentOptions();
        if (confFilePath != null) {
            try {
                String json = FileUtils.readFileToString(new File(confFilePath), "UTF-8");
                JsonObject config = new JsonObject(json);
                deploymentOptions.setConfig(config);
            } catch (IOException e) {
                logger.fatal(e.getMessage(), e);
            }
        }


        ClientManager[] managers = new ClientManager[Runtime.getRuntime().availableProcessors()];
        for (int i = 0; i < managers.length; i += 1) {
            managers[i] = new ClientManager(i);
        }
        Vertx vertx = Vertx.vertx();
        for (ClientManager manager : managers) {
            vertx.deployVerticle(manager, deploymentOptions);
        }
        vertx.deployVerticle(Tracer.class.getName());
    }

    public static void stop(String[] args) {
        System.exit(0);
    }

}
