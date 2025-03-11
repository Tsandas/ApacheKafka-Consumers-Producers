package kafka.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        Properties properties = new Properties();
        //Running on docker compose
        properties.setProperty("bootstrap.servers", "localhost:19092");

        //producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // properties.setProperty("partitioner.class", "RoundRobinPartitioner"); //dont use it

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int j=0;j<10;j++) {
            for (int i = 0; i < 30; i++) {
                //create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test_topic2", "Hello World" + i);
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record successfully sends or an exception is thrown
                        if (e == null) {
                            // the record was successfully send
                            log.info("Recieved new metadata \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Parition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp() + "\n");
                        } else {
                            log.error("Error while sending data", e);
                        }
                    }
                });


            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //flush and close
        //tell the producer to send all data and block until done
        producer.flush();

        producer.close();
        log.info("Closing ProducerDemo");

    }
}
