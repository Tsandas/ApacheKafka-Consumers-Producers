package kafka.demos;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        Properties properties = new Properties();
        //Running on docker compose
        properties.setProperty("bootstrap.servers", "localhost:19092");

        //producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("test_topic2", "Hello World");

        //send data
        producer.send(producerRecord);

        //flush and close
        //tell the producer to send all data and block until done
        producer.flush();

        producer.close();
        log.info("Closing ProducerDemo");

    }
}
