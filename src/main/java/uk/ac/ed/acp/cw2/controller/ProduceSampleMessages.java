import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProduceSampleMessages {

    public static void main(String[] args) {

        // 1. Configure Kafka Producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Change if needed
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 0);

        // 2. Create KafkaProducer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // 3. Our sample data
            //  - AAA (3 chars) => "good"
            //  - ABCD (4 chars) => "good"
            //  - ABCDE (5 chars) => "bad"
            // Each has a value = 10.5
            String[] keys = { "AAA", "ABCD", "ABCDE" };
            double value = 10.5;

            for (String k : keys) {
                // Build the message JSON
                // "uid" is your student ID, "comment" can be "" or anything
                String messageJson = String.format(
                        "{" +
                                "\"uid\":\"s2693586\"," +
                                "\"key\":\"%s\"," +
                                "\"comment\":\"\"," +
                                "\"value\":%.1f" +
                                "}",
                        k,
                        value
                );

                // 4. Send it to "testTopic"
                ProducerRecord<String, String> record = new ProducerRecord<>("testTopic", messageJson);
                producer.send(record);
            }

            // 5. Flush and close
            producer.flush();
            System.out.println("Successfully produced 3 messages to testTopic");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
