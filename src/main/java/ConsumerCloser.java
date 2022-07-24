import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerCloser implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCloser.class);

    private final ConsumerWorker consumerWorker;

    ConsumerCloser(final ConsumerWorker consumerWorker) {
        this.consumerWorker = consumerWorker;
    }

    @Override
    public void run() {
        try {
            consumerWorker.shutdown();
        } catch (InterruptedException e) {
            log.error("Error shutting down consumer", e);
        }
    }
}
