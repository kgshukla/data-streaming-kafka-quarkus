package org.acme;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

@RestController
public class DataProducer {

    private final String LEFT_STREAM_TOPIC = "left-stream-topic";
    private final String RIGHT_STREAM_TOPIC = "right-stream-topic";

    private static final Map<Integer, ATDData> ATD;
    static {
        ATD = new HashMap<>();
        ATD.put(1, new ATDData("A", "ATD Txn - A"));
        ATD.put(2, new ATDData("B", "ATD Txn - B"));
        ATD.put(3, new ATDData("C", "ATD Txn - C"));
        ATD.put(4, new ATDData("D", "ATD Txn - D"));
        ATD.put(5, new ATDData("E", "ATD Txn - E"));
        ATD.put(6, new ATDData("F", "ATD Txn - F"));
        ATD.put(7, new ATDData("G", "ATD Txn - G"));
        ATD.put(8, new ATDData("H", "ATD Txn - H"));
    }


    private static final Map<Integer, RNData> RN;
    static {
        RN = new HashMap<Integer, RNData>();
        RN.put(1, new RNData("A", "RN Txn - A"));
        RN.put(2, new RNData("B", "RN Txn - B"));
        RN.put(3, new RNData("C", "RN Txn - C"));
        RN.put(4, new RNData("D", "RN Txn - D"));
        RN.put(5, new RNData("E", "RN Txn - E"));
        RN.put(6, new RNData("F", "RN Txn - F"));
        RN.put(7, new RNData("G", "RN Txn - G"));
        RN.put(8, new RNData("H", "RN Txn - H"));
        RN.put(9, new RNData("I", "RN Txn - I"));
        RN.put(10, new RNData("J", "RN Txn - J"));
    }

    private Properties props;
    Producer<String, String> producer;

    private void setup() {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    @RequestMapping("/sendmanyrecords")
    public void sendManyRecords() {
        setup();
        try {
            for (int i = 0; i < 10000; i++) { 
                producer.send(new ProducerRecord<String, String>(LEFT_STREAM_TOPIC,
                                                                    "A".concat(String.valueOf(i)),
                                                                    "ATD - txn ".concat(String.valueOf(i)) ));

                try {
                    Thread.sleep(3);
                } catch (InterruptedException e) {}
                
                producer.send(new ProducerRecord<String, String>(RIGHT_STREAM_TOPIC,
                                                                    "A".concat(String.valueOf(i)),
                                                                    "RN - txn ".concat(String.valueOf(i))));
            }
        } finally {
            producer.close();
        }
    }

    @RequestMapping("/sendfewrecords")
    public void sendFewRecords() {
        setup();
        int max = 3;
        int min = 1;

        try {
            for (int i = 0; i < 10; i++) {
                if (ATD.containsKey(i + 1)) {
                    ATDData data = ATD.get(i+1);
                    producer.send(new ProducerRecord<String, String>(LEFT_STREAM_TOPIC, 
                                                                        data.getKey(), 
                                                                        data.getValue()));
                }
                if (RN.containsKey(i + 1)) {
                    int sleep_time = (int)(Math.random()*((max-min)+1))+min;
                    try {
                        Thread.sleep(sleep_time*1000);
                    } catch (InterruptedException e) {}

                    RNData rnData = RN.get(i+1);
                    producer.send(new ProducerRecord<String, String>(RIGHT_STREAM_TOPIC, 
                                                                        rnData.getKey(), 
                                                                        rnData.getValue()));
                }
            }
        } finally {
            producer.close();
        }
    }

    @RequestMapping("/sendoneleftrecord")
    public void sendOneLeftRecord() {
        setup();
        try {
            for (int i = 0; i < 1; i++) {
                if (ATD.containsKey(i + 1)) {
                    ATDData data = ATD.get(i+1);
                    producer.send(new ProducerRecord<String, String>(LEFT_STREAM_TOPIC, 
                                                                        data.getKey(), 
                                                                        data.getValue()));
                }
            }
        } finally {
            producer.close();
        }

    }
}