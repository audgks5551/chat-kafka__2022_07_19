import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ConsumerWorker implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);

    private CountDownLatch countDownLatch;
    private Consumer<String, String> consumer;

    private List<String> messages;

    private List<String> topics;

    public ConsumerWorker(Consumer<String, String> consumer, List<String> messages, List<String> topics) {
        this.consumer = consumer;
        this.messages = messages;
        this.topics = topics;
        countDownLatch = new CountDownLatch(1);
    }

    @Override
    public void run() {
        consumer.subscribe(topics);

        try {
            while (true) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                if (consumerRecords != null) {
                    for (final ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        messages.add(consumerRecord.value());
                    }
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer poll woke up");
        } finally {
            consumer.close();
            countDownLatch.countDown();
        }
    }

    void shutdown() throws InterruptedException {
        consumer.wakeup();
        countDownLatch.await();
        log.info("Consumer closed");
    }
}
