import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;

public class ChatRepository {
    private KafkaRepository kafkaRepository;
    private ObjectMapper objectMapper = new ObjectMapper();
    public final static String CHAT_LIST_TOPIC_NAME = "chatList";

    public ChatRepository() {
        kafkaRepository = new KafkaRepository();
    }

    public Room createRoom(Room room) {

        if (!kafkaRepository.createTopic(room.getId())) {
            return null;
        }

        String key = room.getId();
        String value = null;
        try {
            value = objectMapper.writeValueAsString(room);
        } catch (JsonProcessingException e) {
            return null;
        }

        RecordMetadata recordMetadata =
                kafkaRepository.sendStringString(CHAT_LIST_TOPIC_NAME, key, value);

        if (recordMetadata == null) {
            return null;
        }

        return room;
    }

    public boolean deleteRoom(Room room) {
        return kafkaRepository.deleteTopic(room.getId());
    }

    public void saveMessage(Room room, String message) {
        kafkaRepository.sendStringString(room.getId(), null, message);
    }

    public void continueMessage(Room room, List<String> messages) {
        kafkaRepository.continueLoadingStringStringConsumer(room.getId(), messages);
    }
}
