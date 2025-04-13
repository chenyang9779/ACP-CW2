package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.dto.TransformMessagesRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@RestController
public class TransformMessagesController {

    private static final Logger logger = LoggerFactory.getLogger(TransformMessagesController.class);

    private final RuntimeEnvironment environment;
    private final ConnectionFactory connectionFactory;
    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper;

    public TransformMessagesController(RuntimeEnvironment environment) {
        this.environment = environment;

        this.connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(environment.getRabbitMqHost());
        connectionFactory.setPort(environment.getRabbitMqPort());

        this.jedisPool = new JedisPool(environment.getRedisHost(), environment.getRedisPort());

        this.objectMapper = new ObjectMapper();
    }

    /**
     * POST /transformMessages
     * <p>
     * Body:
     * {
     * "readQueue": "inboundQueue",
     * "writeQueue": "outboundQueue",
     * "messageCount": 5
     * }
     * <p>
     * Steps:
     * - read {messageCount} messages from {readQueue}
     * - parse each message as "normal" or "tombstone"
     * - if normal & version is new => store in redis & add 10.5 => write to {writeQueue}
     * - if normal & version is old/same => pass unmodified => {writeQueue}
     * - if tombstone => remove key from redis => write a special stats packet to {writeQueue}
     */
    @PostMapping("/transformMessages")
    public ResponseEntity<Void> transformMessages(@RequestBody TransformMessagesRequest request) {

        int totalMessagesProcessed = 0;
        int totalMessagesWritten = 0;
        int totalRedisUpdates = 0;
        double totalValueWritten = 0.0;
        double totalAdded = 0.0;

        int needed = request.getMessageCount();

        try (
                Connection rabbitConnection = connectionFactory.newConnection();
                Channel channel = rabbitConnection.createChannel();
                Jedis jedis = jedisPool.getResource()
        ) {

            channel.queueDeclare(request.getReadQueue(), false, false, false, null);
            channel.queueDeclare(request.getWriteQueue(), false, false, false, null);

            while (needed > 0) {

                GetResponse response = channel.basicGet(request.getReadQueue(), true);
                if (response == null) {

                    Thread.sleep(100);
                    continue;
                }

                String msgBody = new String(response.getBody(), StandardCharsets.UTF_8);
                totalMessagesProcessed++;

                Map<String, Object> messageMap = objectMapper.readValue(msgBody, Map.class);

                if (!messageMap.containsKey("version") || !messageMap.containsKey("value")) {

                    String tombstoneKey = (String) messageMap.get("key");

                    jedis.del(tombstoneKey);

                    Map<String, Object> tombstoneStats = new HashMap<>();
                    tombstoneStats.put("totalMessagesWritten", totalMessagesWritten);
                    tombstoneStats.put("totalMessagesProcessed", totalMessagesProcessed);
                    tombstoneStats.put("totalRedisUpdates", totalRedisUpdates);
                    tombstoneStats.put("totalValueWritten", totalValueWritten);
                    tombstoneStats.put("totalAdded", totalAdded);

                    String json = objectMapper.writeValueAsString(tombstoneStats);
                    channel.basicPublish("", request.getWriteQueue(), null, json.getBytes(StandardCharsets.UTF_8));


                    totalMessagesWritten++;
                } else {

                    String normalKey = (String) messageMap.get("key");
                    int newVersion = ((Number) messageMap.get("version")).intValue();
                    double oldValue = ((Number) messageMap.get("value")).doubleValue();

                    String storedVersionStr = jedis.get(normalKey);
                    if (storedVersionStr == null) {

                        jedis.set(normalKey, String.valueOf(newVersion));
                        totalRedisUpdates++;

                        double updatedValue = oldValue + 10.5;
                        totalAdded += 10.5;

                        messageMap.put("value", updatedValue);

                        String json = objectMapper.writeValueAsString(messageMap);
                        channel.basicPublish("", request.getWriteQueue(), null, json.getBytes(StandardCharsets.UTF_8));


                        totalMessagesWritten++;
                        totalValueWritten += updatedValue;
                    } else {

                        int storedVersion = Integer.parseInt(storedVersionStr);

                        if (newVersion > storedVersion) {

                            jedis.set(normalKey, String.valueOf(newVersion));
                            totalRedisUpdates++;

                            double updatedValue = oldValue + 10.5;
                            totalAdded += 10.5;

                            messageMap.put("value", updatedValue);
                            String json = objectMapper.writeValueAsString(messageMap);
                            channel.basicPublish("", request.getWriteQueue(), null, json.getBytes(StandardCharsets.UTF_8));


                            totalMessagesWritten++;
                            totalValueWritten += updatedValue;
                        } else {

                            String json = objectMapper.writeValueAsString(messageMap);
                            channel.basicPublish("", request.getWriteQueue(), null, json.getBytes(StandardCharsets.UTF_8));


                            totalMessagesWritten++;
                            totalValueWritten += oldValue;
                        }
                    }
                }

                needed--;
            }

            return ResponseEntity.ok().build();

        } catch (Exception e) {
            logger.error("Error while transforming messages", e);
            return ResponseEntity.status(500).build();
        }
    }

    @GetMapping("/redis/{queueName}")
    public ResponseEntity<List<String>> readRedisQueue(@PathVariable String queueName) {
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> items = jedis.lrange(queueName, 0, -1);
            return ResponseEntity.ok(items);
        } catch (Exception e) {
            logger.error("Failed to read Redis queue {}", queueName, e);
            return ResponseEntity.status(500).build();
        }
    }

}
