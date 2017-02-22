package kafka;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class ProducerTest{
    public static void main(String[] args) {
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "node6:9092,node7:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer.class", "kafka.IntegerEncoder");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<Integer, String> producer = new Producer<>(config);
 
        for (int nEvents = 0; nEvents < 100; nEvents++) { 
               String msg = "add: notification" + nEvents + ":30:seconds"; 
               KeyedMessage<Integer, String> data = new KeyedMessage<Integer, String>("notifications",nEvents, msg);
               producer.send(data);
        }
        producer.close();
    }
}
