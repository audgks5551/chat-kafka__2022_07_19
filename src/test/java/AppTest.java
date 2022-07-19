import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class AppTest {
    private KafkaRepository kafkaRepository;

    public AppTest() {
        kafkaRepository = new KafkaRepository();
    }

    @AfterAll
    public void afterAll() {
        kafkaRepository.delete("chat1");
        kafkaRepository.delete("chat2");
        kafkaRepository.delete("chat3");
    }

    @Test
    public void 토픽_생성() throws InterruptedException {
        // given
        String topicName = "chat1";
        kafkaRepository.delete(topicName);
        Thread.sleep(1000);

        // when
        boolean create = kafkaRepository.create(topicName);

        // then
        assertThat(create).isTrue();
    }

    @Test
    public void 토픽_리스트_조회() {
        // given
        String topicName = "chat2";
        kafkaRepository.create(topicName);

        // when
        List<String> topicList = kafkaRepository.topicList();

        // then
        assertThat(topicList).contains("chat2");
    }

    @Test
    public void 토픽_상세조회() {
        // given
        String topicName = "chat3";
        kafkaRepository.create(topicName);

        // when
        TopicDescription topic = kafkaRepository.topicDescribe(topicName);

        // then
        TopicPartitionInfo topicPartitionInfo = topic.partitions().get(0);
        int partitionSize = topic.partitions().size();
        int replicasSize = topicPartitionInfo.replicas().size();

        assertThat(topic.name()).isEqualTo(topicName);
        assertThat(partitionSize).isEqualTo(1);
        assertThat(replicasSize).isEqualTo(1);
    }
}
