package io.nhannt22.demo.job;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String args[]) throws Exception {
        consumeKafka();
    }

    public <E> KafkaSource<Row> consumeKafka(
            String topic,
            String groupId,
            Properties consumerConfig,
            OffsetResetStrategy offsetResetStrategy
    ) {
        KafkaSource<Row> kafkaSource = KafkaSource.<Row>builder()
                .setTopics(topic)
                .setGroupId(groupId)
                .setProperties(consumerConfig)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(offsetResetStrategy))
                .build();
        return kafkaSource;
    }

    public static void consumeKafka() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String groupId = "my-java-application";

        String topic = "raw_tm.vault.api.v1.postings.posting_instruction_batch.created";

        // create Producer Properties
//        Properties properties = new Properties();

        // connect to Conduktor Playground
//        properties.setProperty("bootstrap.servers", "54.169.46.195:9094");
        // properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.mechanism", "PLAIN");

        // create consumer configs
//        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
//        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
//        properties.setProperty("group.id", groupId);
//        properties.setProperty("auto.offset.reset", "earliest");

        Properties consumerConfig = new Properties();
        consumerConfig.put("bootstrap.servers", "54.169.46.195:9094");
        consumerConfig.put("sasl.mechanism", "PLAIN");
        consumerConfig.put("group.id", groupId);
        consumerConfig.put("auto.offset.reset", "earliest");
        
        //        consumerConfig.put("key.deserializer", StringDeserializer.class.getName());


//        consumerConfig.put("security.protocol", "SASL_SSL");
//        consumerConfig.put("sasl.mechanism", "AWS_MSK_IAM");
//        consumerConfig.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
//        consumerConfig.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        // create a consumer
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        ConsumerDemo flinkApp = new ConsumerDemo();

        KafkaSource<Row> kafkPostingIntruction = flinkApp.consumeKafka(
                "raw_tm.vault.api.v1.postings.posting_instruction_batch.created",
                groupId,
                consumerConfig,
                OffsetResetStrategy.EARLIEST);

//        // subscribe to a topic
//        consumer.subscribe(Arrays.asList(topic));
//
//        while (true) {
//
//            log.info("Polling");
//
//            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
//
//            for (ConsumerRecord<String, String> record : records) {
//                log.info("Key: " + record.key() + ", Value: " + record.value());
//                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
//            }
//        }
    }
}