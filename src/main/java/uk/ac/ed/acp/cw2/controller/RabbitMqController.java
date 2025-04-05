package uk.ac.ed.acp.cw2.controller;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.impl.Environment;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CheckedInputStream;


/**
 * RabbitMqController is a REST controller that provides endpoints for sending and receiving stock symbols
 * through RabbitMQ. This class interacts with a RabbitMQ environment which is configured dynamically during runtime.
 */
@RestController()
@RequestMapping("/rabbitMq")
public class RabbitMqController {

    private final Logger logger = LoggerFactory.getLogger(RabbitMqController.class);
    private ConnectionFactory connectionFactory = null;

    public RabbitMqController(RuntimeEnvironment environment) {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(environment.getRabbitMqHost());
        connectionFactory.setPort(environment.getRabbitMqPort());
    }

    @PutMapping("/{qeueuName}/{messageCount}")
    public ResponseEntity<Void> put(@PathVariable String qeueuName, @PathVariable String messageCount) {
        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(qeueuName, false, false, false, null);

            for (int i = 0; i < Integer.parseInt(messageCount); i++) {
                String messageBody = String.format("{\"uid\":\"s2693586\", \"count\":%d}", i);
                channel.basicPublish("", qeueuName, null, messageBody.getBytes(StandardCharsets.UTF_8));
            }
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            logger.error("Failed to send message to RabbitMQ", e);
            throw new RuntimeException("Failed to send message to RabbitMQ");
        }
    }

    @GetMapping("/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> getMessages(@PathVariable String queueName, @PathVariable String timeoutInMsec) {
        List<String> messages = new ArrayList<>();
        long endTime = System.currentTimeMillis() + Long.parseLong(timeoutInMsec);

        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                logger.info(delivery.getProperties().toString());
                String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
                messages.add(messageBody);
            };
            String consumerTag = channel.basicConsume(queueName, true, deliverCallback, consumerTag1 -> {
            });

            while (System.currentTimeMillis() < endTime) {
                Thread.sleep(50);
            }

            channel.basicCancel(consumerTag);
            return ResponseEntity.ok(messages);
        } catch (Exception e) {
            logger.error("Failed to retrieve message from RabbitMQ", e);
            throw new RuntimeException("Failed to retrieve message from RabbitMQ");
        }
    }

    @PutMapping("/testTransformData/{queueName}")
    public ResponseEntity<Void> putTestTransformMessages(@PathVariable String queueName) {
        try (
                Connection connection = connectionFactory.newConnection();
                Channel channel = connection.createChannel()
        ) {
            channel.queueDeclare(queueName, false, false, false, null);

            ObjectMapper objectMapper = new ObjectMapper();

            List<Map<String, Object>> testMessages = getMaps();

            for (Map<String, Object> msg : testMessages) {
                String json = objectMapper.writeValueAsString(msg);
                channel.basicPublish("", queueName, null, json.getBytes(StandardCharsets.UTF_8));
            }

            return ResponseEntity.ok().build();

        } catch (Exception e) {
            logger.error("Failed to publish test transform messages", e);
            return ResponseEntity.status(500).build();
        }
    }

    private static List<Map<String, Object>> getMaps() {
        Map<String, Object> msg1 = Map.of("key", "ABC", "version", 1, "value", 100.0);
        Map<String, Object> msg2 = Map.of("key", "ABC", "version", 1, "value", 200.0);
        Map<String, Object> msg3 = Map.of("key", "ABC", "version", 3, "value", 400.0);
        Map<String, Object> msg4 = Map.of("key", "ABC", "version", 2, "value", 200.0);
        Map<String, Object> msg5 = Map.of("key", "ABC");
        Map<String, Object> msg6 = Map.of("key", "ABC", "version", 2, "value", 200.0);

        List<Map<String, Object>> testMessages = List.of(msg1, msg2, msg3, msg4, msg5, msg6);
        return testMessages;
    }

}
