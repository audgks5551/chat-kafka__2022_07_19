import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ChatRepository {
    private KafkaRepository kafkaRepository;
    private ObjectMapper objectMapper = new ObjectMapper();
    public final static String CHAT_LIST_TOPIC_NAME = "chatList";

    public ChatRepository() {
        kafkaRepository = new KafkaRepository();
    }

    public Room create(Room room) {

        if (!kafkaRepository.createTopic(room.getId())) {
            return null;
        }

        String value = null;
        try {
            value = objectMapper.writeValueAsString(room);
        } catch (JsonProcessingException e) {
            return null;
        }

        RecordMetadata recordMetadata =
                kafkaRepository.sendStringString(CHAT_LIST_TOPIC_NAME, value);

        if (recordMetadata == null) {
            return null;
        }

        return room;
    }

    public boolean delete(Room room) {
        return kafkaRepository.deleteTopic(room.getId());
    }
}
