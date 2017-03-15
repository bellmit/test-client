package pku.netlab.hermes;

/**
 * Created by hult on 3/14/17.
 */
public class Counter {
    static long start = -1;
    static long end = -1;
    static long deltaTime;
    static long total = 0;
    static double rate = 0;
    public void update() {
        total += 1;
        if (start == -1) {
            start = System.currentTimeMillis();
        }
        end = System.currentTimeMillis();
        deltaTime = end - start;
        rate = total * 1000 / deltaTime;
    }

    public long result() {
        return (long) rate;
    }

    @Override
    public String toString() {
        return String.format("start: %s, end: %s, deltaTime: %s, total: %s, rate: %s",
                start, end, deltaTime, total, rate);
    }
}
