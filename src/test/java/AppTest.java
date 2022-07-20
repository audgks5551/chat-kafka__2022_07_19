import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class AppTest {
    private KafkaRepository kafkaRepository;
    private ChatRepository chatRepository;

    public AppTest() {
        kafkaRepository = new KafkaRepository();
        chatRepository = new ChatRepository();

        kafkaRepository.createTopic(ChatRepository.CHAT_LIST_TOPIC_NAME);
    }

    @AfterAll
    public void afterAll() {
        kafkaRepository.deleteTopic("chat1");
        kafkaRepository.deleteTopic("chat2");
        kafkaRepository.deleteTopic("chat3");
        kafkaRepository.deleteTopic(ChatRepository.CHAT_LIST_TOPIC_NAME);
    }

    @Test
    public void 토픽_생성() throws InterruptedException {
        // given
        String topicName = "chat1";
        kafkaRepository.deleteTopic(topicName);
        Thread.sleep(1000);

        // when
        boolean create = kafkaRepository.createTopic(topicName);

        // then
        assertThat(create).isTrue();
    }

    @Test
    public void 토픽_리스트_조회() {
        // given
        String topicName = "chat2";
        kafkaRepository.createTopic(topicName);

        // when
        List<String> topicList = kafkaRepository.listTopic();

        // then
        assertThat(topicList).contains("chat2");
    }

    @Test
    public void 토픽_상세조회() {
        // given
        String topicName = "chat3";
        kafkaRepository.createTopic(topicName);

        // when
        TopicDescription topic = kafkaRepository.describeTopic(topicName);

        // then
        TopicPartitionInfo topicPartitionInfo = topic.partitions().get(0);
        int partitionSize = topic.partitions().size();
        int replicasSize = topicPartitionInfo.replicas().size();

        assertThat(topic.name()).isEqualTo(topicName);
        assertThat(partitionSize).isEqualTo(1);
        assertThat(replicasSize).isEqualTo(1);
    }

    @Test
    public void producer_생성() {
        // when
        KafkaProducer kafkaProducer = kafkaRepository.createStringStringProducer();

        // then
        assertThat(kafkaProducer).isNotNull();

        kafkaRepository.deleteProducer(kafkaProducer);
    }

    @Test
    public void producer_데이터_한개_보내기() {
        // given
        KafkaProducer<String, String> producer = kafkaRepository.createStringStringProducer();
        String data = "안녕";

        // when
        RecordMetadata recordMetadata =
                kafkaRepository.sendStringStringProducer(producer, ChatRepository.CHAT_LIST_TOPIC_NAME, data);

        // then
        assertThat(recordMetadata).isNotNull();

        kafkaRepository.deleteProducer(producer);
    }

    @Test
    public void 채팅방_생성하기() {
        // given
        String id = UUID.randomUUID().toString();
        String name = "같이 공부하실 분";
        Room room = new Room(id, name);

        // when
        Room createdRoom = chatRepository.create(room);

        // then
        assertThat(createdRoom).isNotNull();

        kafkaRepository.deleteTopic(createdRoom.getId());
    }

    @Test
    public void 채팅방_삭제하기() {
        // given
        String id = UUID.randomUUID().toString();
        String name = "같이 공부하실 분";
        Room room = new Room(id, name);
        Room createdRoom = chatRepository.create(room);

        // when
        boolean check = chatRepository.delete(createdRoom);

        // then
        assertThat(check).isTrue();
    }
}
