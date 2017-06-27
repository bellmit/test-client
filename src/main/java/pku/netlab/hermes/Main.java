package pku.netlab.hermes;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CLIException;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonObject;
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

    public static void main(String[] args) throws IOException{
        CommandLine commandLine = cli(args);
        if (commandLine == null) {
            System.out.println("no config file");
            System.exit(-1);
        }

        String confFilePath = commandLine.getOptionValue("c");

        DeploymentOptions deploymentOptions = new DeploymentOptions();
        if (confFilePath == null) {
            System.out.println("config file should not be empty");
            System.exit(0);
        }
        String jsonStr = FileUtils.readFileToString(new File(confFilePath), "UTF-8");
        Config config = new Config(new JsonObject(jsonStr));

        ClientManager[] managers = new ClientManager[config.getInstanceNum()];
        for (int i = 0; i < managers.length; i += 1) {
            managers[i] = new ClientManager(i, config);
        }
        Vertx vertx = Vertx.vertx();
        for (ClientManager manager : managers) {
            vertx.deployVerticle(manager);
        }
        vertx.deployVerticle(Tracer.class.getName());
    }

    public static void stop(String[] args) {
        System.exit(0);
    }

}
