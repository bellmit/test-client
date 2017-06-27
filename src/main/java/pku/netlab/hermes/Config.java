package pku.netlab.hermes;

import io.vertx.core.json.JsonObject;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Random;
import java.util.stream.Stream;

/**
 * Created by hult on 2017/6/27.
 */
public class Config {
    JsonObject conf;
    private final String payload;
    public Config(JsonObject object) {
        this.conf = object;
        this.payload = new String(new byte[1000]);
    }
    public String[] getHosts (){
        return conf.getString("host").split(",");
    }

    public int getPort() {
        return conf.getInteger("port");
    }

    public int getInterval() {
        return conf.getInteger("interval");
    }

    public int getClientNum() {
        return conf.getInteger("client_num") / Runtime.getRuntime().availableProcessors();
    }

    public String getPayload() {
        return this.payload;
    }

    public int getQPS() {
        return conf.getInteger("qps");
    }

    public int getDuration() {
        return conf.getInteger("duration");
    }

    public String[] getUsernamePwd() {
        String[] ret = new String[2];
        String username = conf.getString("username");
        if (username != null || username.length() != 0) {
            ret[0] = username;
            ret[1] = conf.getString("password");
        }
        return ret;
    }

    public String getIPAddr() {
        String networkID = this.conf.getString("network");
        Stream<String> ipStream = Stream.empty();
        try {
            Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
            while (enumeration.hasMoreElements()) {
                NetworkInterface ni = enumeration.nextElement();
                ipStream = Stream.concat(ipStream, ni.getInterfaceAddresses().stream().map(ia -> ia.getAddress().getHostAddress()));
            }
        } catch (SocketException e) {
            System.out.println(e);
            System.exit(0);
        }
        String ret = ipStream.filter(ip -> ip.startsWith(networkID)).findFirst().get();
        if (ret == null) {
            System.out.println("Error in network prefix");
            System.exit(0);
        }
        return ret;
    }

    public String randomHost() {
        String[] hosts = getHosts();
        int i = new Random().nextInt(hosts.length);
        return hosts[i];
    }
}
