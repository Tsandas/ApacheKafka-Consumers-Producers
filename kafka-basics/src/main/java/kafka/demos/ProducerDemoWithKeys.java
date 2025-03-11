package kafka.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

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


        for(int j =0 ; j<10;j++) {
            for (int i = 0; i < 100; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World " + i;

                //create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                //send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes every time a record successfully sends or an exception is thrown
                        if (e == null) {
                            // the record was successfully send
                            log.info(
                                    "Key: " + key + " | Partition: " + recordMetadata.partition() + "Topic: " + recordMetadata.topic()
                            );
                        } else {
                            log.error("Error while sending data", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(600);
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
