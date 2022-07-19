import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaRepository {
    private final static String BOOTSTRAP_SERVER = "192.168.0.21:9092";
    private Properties configs;
    public KafkaRepository() {
        configs = new Properties();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    }

    public boolean create(String topicName) {

        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

        try (AdminClient admin = AdminClient.create(configs)) {
            CreateTopicsResult result = admin.createTopics(Arrays.asList(newTopic));

            Void unused = result.all().get();
            System.out.println(unused);

        } catch (ExecutionException e) {
            return false;
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    public boolean delete(String topicName) {

        try (AdminClient admin = AdminClient.create(configs)) {
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

    public List<String> topicList() {
        try (AdminClient admin = AdminClient.create(configs)) {
            return admin
                    .listTopics()
                    .names()
                    .get()
                    .stream().toList();
        } catch (ExecutionException e) {
            return null;
        } catch (InterruptedException e) {
            return null;
        }
    }

    public TopicDescription topicDescribe(String topicName) {
        try (AdminClient admin = AdminClient.create(configs)) {
            return admin
                    .describeTopics(Arrays.asList(topicName))
                    .topicNameValues()
                    .get(topicName)
                    .get();

        } catch (ExecutionException e) {
            return null;
        } catch (InterruptedException e) {
            return null;
        }
    }
}
