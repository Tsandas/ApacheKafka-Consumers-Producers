package kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");
        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();
        //Running on docker compose
        properties.setProperty("bootstrap.servers", "localhost:19092");

        //create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); //none/earliest/latest
        //none means that if we dont have existing consumer group, it will fail, we must set it before
        //earliest we start from the beggining of the topic
        //latest offset
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        // properties.setProperty("group.instance.id", "...."); strategy for static assignments

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        //get a reference to main thread
        final Thread mainThread = Thread.currentThread();

        //adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
           public void run(){
               log.info("Detected shutdown hook, exiting by calling consumer.wakeup()");
               consumer.wakeup();

               // join the main thread to allow the execution of the code in the main thread
               try {
                   mainThread.join();
               } catch (InterruptedException e) {
                   throw new RuntimeException(e);
               }
           }
        });

        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic)); //Arrays.asList(topic,topic2,topic3,...,topicN)

            //poll for data
            while (true) {
                log.info("Polling for data...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //watiting up to 1 sec to recieve data
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() +
                            " | Value: " + record.value() +
                            " | Partition: " + record.partition() +
                            " | Offset: " + record.offset());
                }

            }
        }catch (WakeupException e){
            log.info("Received shutdown signal");
        }catch (Exception e){
            log.error("Unexpected exception",e);
        }finally {
            consumer.close(); // close consumer and commit offsets
            log.info("Closing ConsumerDemo");
        }

    }
}
