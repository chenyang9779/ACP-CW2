package uk.ac.ed.acp.cw2.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * KafkaController is a REST API controller used to interact with Apache Kafka for producing
 * and consuming stock symbol events. This class provides endpoints for sending stock symbols
 * to a Kafka topic and retrieving stock symbols from a Kafka topic.
 * <p>
 * It is designed to handle dynamic Kafka configurations based on the runtime environment
 * and supports security configurations such as SASL and JAAS.
 */
@RestController()
@RequestMapping("/kafka")
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    /**
     * Constructs Kafka properties required for KafkaProducer and KafkaConsumer configuration.
     *
     * @param environment the runtime environment providing dynamic configuration details
     *                     such as Kafka bootstrap servers.
     * @return a Properties object containing configuration properties for Kafka operations.
     */
    private Properties getKafkaProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("enable.auto.commit", "true");
        kafkaProps.put("acks", "all");

        kafkaProps.put("group.id", UUID.randomUUID().toString());
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("enable.auto.commit", "true");

        if (environment.getKafkaSecurityProtocol() != null) {
            kafkaProps.put("security.protocol", environment.getKafkaSecurityProtocol());
        }
        if (environment.getKafkaSaslMechanism() != null) {
            kafkaProps.put("sasl.mechanism", environment.getKafkaSaslMechanism());
        }
        if (environment.getKafkaSaslJaasConfig() != null) {
            kafkaProps.put("sasl.jaas.config", environment.getKafkaSaslJaasConfig());
        }

        return kafkaProps;
    }

    @PutMapping("/{writeTopic}/{messageCount}")
    public ResponseEntity<Void> putMessage(@PathVariable String writeTopic, @PathVariable int messageCount) {
        Properties kafkaProps = getKafkaProperties();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            for (int i = 0; i < messageCount; i++) {
                String message = String.format("{\"uuid\":\"s2693586\", \"count\":%d}", i);
                producer.send(new ProducerRecord<>(writeTopic, message));
            }
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            logger.error("Error while sending message", e);
            throw new RuntimeException("Failed to send messages to kafka");
        }
    }

    @GetMapping("/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<List<String>> readMessages(@PathVariable String readTopic, @PathVariable int timeoutInMsec) {
        Properties kafkaProps = getKafkaProperties();
        List<String> messages = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));
            long endTime = System.currentTimeMillis() + timeoutInMsec;

            while (System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    messages.add(record.value());
                }
            }
            return ResponseEntity.ok(messages);
        }catch (Exception e) {
            logger.error("Failed to consume messages from Kafka", e);
            throw new RuntimeException("Failed to consume messages from Kafka");
        }
    }



}
