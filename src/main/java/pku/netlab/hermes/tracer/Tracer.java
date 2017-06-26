package pku.netlab.hermes.tracer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Counter;

/**
 * Created by hult on 6/26/17.
 */
public class Tracer extends AbstractVerticle{
    Logger logger = LoggerFactory.getLogger(Tracer.class);
    Counter counter;
    @Override
    public void start() throws Exception {
        vertx.sharedData().getCounter("ack", res-> {
            this.counter = res.result();
            vertx.setPeriodic(1_000, id-> {
                counter.get(getC-> {
                    System.out.println(getC.result());
                });
            });
        });
    }
}
