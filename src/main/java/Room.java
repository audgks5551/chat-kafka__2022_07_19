import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Room {
    private String id;
    private String name;

    public Room(String name) {
        this.name = name;
    }
}
