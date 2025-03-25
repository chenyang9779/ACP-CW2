package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.dto.ProcessMessagesRequest;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

@RestController
public class ProcessMessagesController {

    private static final Logger logger = LoggerFactory.getLogger(ProcessMessagesController.class);

    private final RuntimeEnvironment environment;
    private final ConnectionFactory connectionFactory; // For Rabbit
    private final ObjectMapper objectMapper; // For JSON
    private final RestTemplate restTemplate; // For ACP calls
    private Properties kafkaProps;

    public ProcessMessagesController(RuntimeEnvironment environment) {
        this.environment = environment;
        this.connectionFactory = new ConnectionFactory();
        this.connectionFactory.setHost(environment.getRabbitMqHost());
        this.connectionFactory.setPort(environment.getRabbitMqPort());
        this.objectMapper = new ObjectMapper();
        this.restTemplate = new RestTemplate();
    }


    @PostMapping("/processMessages")
    public ResponseEntity<Void> processMessages(@RequestBody ProcessMessagesRequest request) {

        int messagesToRead = request.getMessageCount();
        int messagesRead = 0;
        int consecutiveEmptyPolls = 0;
        int maxConsecutiveEmptyPolls = 25;
        double runningTotalGood = 0.0;
        double runningTotalBad = 0.0;

        Properties kafkaProps = buildKafkaProperties();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
             Connection rabbitConnection = connectionFactory.newConnection();
             Channel rabbitChannel = rabbitConnection.createChannel()) {

            consumer.subscribe(Collections.singletonList(request.getReadTopic()));

            while (messagesRead < messagesToRead) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));

                if (records.isEmpty()) {
                    consecutiveEmptyPolls++;

                    if (consecutiveEmptyPolls >= maxConsecutiveEmptyPolls) {
                        logger.warn("No more messages received after {} consecutive polls. Stopping early.",
                                maxConsecutiveEmptyPolls);
                        break;
                    }

                    continue;
                }

                consecutiveEmptyPolls = 0;

                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> msgData = objectMapper.readValue(record.value(), Map.class);

                    String key = (String) msgData.get("key");
                    double value = ((Number) msgData.get("value")).doubleValue();

                    if (key.length() == 3 || key.length() == 4) {
                        runningTotalGood += value;
                        msgData.put("runningTotalValue", runningTotalGood);

                        String uuid = storeInAcp(msgData);

                        msgData.put("uuid", uuid);

                        publishToRabbit(rabbitChannel, request.getWriteQueueGood(), msgData);

                    } else {
                        runningTotalBad += value;

                        publishToRabbit(rabbitChannel, request.getWriteQueueBad(), msgData);
                    }

                    messagesRead++;
                    if (messagesRead >= request.getMessageCount()) {
                        break;
                    }
                }
            }

            Map<String, Object> totalGoodMsg = new HashMap<>();
            totalGoodMsg.put("uid", "s2693586");
            totalGoodMsg.put("key", "TOTAL");
            totalGoodMsg.put("value", runningTotalGood);
            totalGoodMsg.put("comment", "");

            publishToRabbit(rabbitChannel, request.getWriteQueueGood(), totalGoodMsg);

            Map<String, Object> totalBadMsg = new HashMap<>();
            totalBadMsg.put("uid", "s2693586");
            totalBadMsg.put("key", "TOTAL");
            totalBadMsg.put("value", runningTotalBad);
            totalBadMsg.put("comment", "");

            publishToRabbit(rabbitChannel, request.getWriteQueueBad(), totalBadMsg);

            logger.info("Successfully processed {} messages. Good: {}, Bad: {}", messagesRead, runningTotalGood,
                    runningTotalBad);
            return ResponseEntity.ok().build();

        } catch (Exception e) {
            logger.error("Error in processMessages", e);
            return ResponseEntity.status(500).build();
        }
    }

    private Properties buildKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");

        props.put("group.id", UUID.randomUUID().toString());

        if (environment.getKafkaSecurityProtocol() != null) {
            props.put("security.protocol", environment.getKafkaSecurityProtocol());
        }
        if (environment.getKafkaSaslMechanism() != null) {
            props.put("sasl.mechanism", environment.getKafkaSaslMechanism());
        }
        if (environment.getKafkaSaslJaasConfig() != null) {
            props.put("sasl.jaas.config", environment.getKafkaSaslJaasConfig());
        }
        return props;
    }

    private String storeInAcp(Map<String, Object> messageData) {
        String baseUrl = environment.getAcpStorageService();

        try {
            return restTemplate.postForObject(
                    baseUrl + "/api/v1/blob",
                    messageData,
                    String.class);
        } catch (Exception e) {
            logger.error("Failed to store in ACP", e);
            throw new RuntimeException("Could not store message in ACP", e);
        }
    }

    private void publishToRabbit(Channel channel, String queueName, Map<String, Object> data) throws IOException {
        channel.queueDeclare(queueName, false, false, false, null);

        String payload = objectToJson(data);

        channel.basicPublish("", queueName, null, payload.getBytes());
    }

    private String objectToJson(Object data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (Exception e) {
            logger.error("Error converting object to JSON", e);
            return "{}";
        }
    }
}
