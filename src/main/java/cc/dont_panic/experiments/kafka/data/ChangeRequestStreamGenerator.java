package cc.dont_panic.experiments.kafka.data;

import java.util.Random;
import java.util.stream.Stream;

public class ChangeRequestStreamGenerator {

    private final Random random;
    private final int maxId;
    private final int maxPropertyNumber;

    public ChangeRequestStreamGenerator(Random random, int maxId, int maxPropertyNumber) {
        this.random = random;
        this.maxId = maxId;
        this.maxPropertyNumber = maxPropertyNumber;
    }

    public Stream<ChangeRequest> createStream() {
        return Stream.generate(() -> {
            long id = random.nextLong(maxId) + 1;
            String propertyName = "property" + random.nextInt(maxPropertyNumber + 1);
            String propertyValue = "value-" + (System.currentTimeMillis() / 1000 / id);
            return new ChangeRequest(id, propertyName, propertyValue);
        });
    }

}
