import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaRepository {
    private final static String BOOTSTRAP_SERVER = "192.168.0.21:9092";
    private Properties topicConfigs;
    private Properties producerConfigs;
    public KafkaRepository() {
        topicConfigs = new Properties();
        topicConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        producerConfigs = new Properties();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    public boolean createTopic(String topicName) {
        try (AdminClient admin = AdminClient.create(topicConfigs)) {
            CreateTopicsResult result =
                    admin.createTopics(Arrays.asList(new NewTopic(topicName, 1, (short) 1)));
            result.all().get();
        } catch (ExecutionException e) {
            return false;
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    public boolean deleteTopic(String topicName) {
        try (AdminClient admin = AdminClient.create(topicConfigs)) {
            DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(Arrays.asList(topicName));

            deleteTopicsResult.all().get();
        } catch (ExecutionException e) {
            String simpleName = e.getCause().getClass().getSimpleName();

            if ("UnknownTopicOrPartitionException".equals(simpleName)) {
                return true;
            }

            return false;
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    public List<String> listTopic() {
        try (AdminClient admin = AdminClient.create(topicConfigs)) {
            return admin
                    .listTopics()
                    .names()
                    .get(1, TimeUnit.SECONDS)
                    .stream().toList();
        } catch (ExecutionException e) {
            return null;
        } catch (InterruptedException e) {
            return null;
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public TopicDescription describeTopic(String topicName) {
        try (AdminClient admin = AdminClient.create(topicConfigs)) {
            return admin
                    .describeTopics(Arrays.asList(topicName))
                    .topicNameValues()
                    .get(topicName)
                    .get(1, TimeUnit.SECONDS);

        } catch (ExecutionException e) {
            return null;
        } catch (InterruptedException e) {
            return null;
        } catch (TimeoutException e) {
            return null;
        }
    }

    public KafkaProducer<String, String> createStringStringProducer() {
        return new KafkaProducer<String, String>(producerConfigs);
    }

    public void deleteProducer(KafkaProducer kafkaProducer) {
        kafkaProducer.close();
    }

    public RecordMetadata sendStringStringProducer(KafkaProducer<String, String> kafkaProducer, String topicName, String data) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, data);

        try {
            return kafkaProducer.send(record).get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            return null;
        }
    }

    public RecordMetadata sendStringString(String topicName, String value) {
        KafkaProducer<String, String> producer = createStringStringProducer();
        RecordMetadata recordMetadata = sendStringStringProducer(producer, topicName, value);
        deleteProducer(producer);
        return recordMetadata;
    }
}
