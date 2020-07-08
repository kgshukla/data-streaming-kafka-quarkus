package org.acme;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

import javax.inject.Inject;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueStore;

@RestController
public class KafkaStreaming {

    private KafkaStreams streamsOuterJoin;
    private final String LEFT_STREAM_TOPIC = "left-stream-topic";
    private final String RIGHT_STREAM_TOPIC = "right-stream-topic";
    private final String OUTER_JOIN_STREAM_OUT_TOPIC = "stream-stream-outerjoin";
    private final String PROCESSED_STREAM_OUT_TOPIC = "processed-topic";
    private final String STORE_NAME = "missing_data_store";

    // private final String INNER_JOIN_STREAM_OUT_TOPIC = "stream-stream-innerjoin";
    // private KafkaStreams streamsInnerJoin;

    private final String KAFKA_APP_ID = "outerjoin";
    private final String KAFKA_SERVER_NAME = "localhost:9092";

    @Inject
    InteractiveQueries interactiveQueries;

    @RequestMapping("/startstream/")
    public void startStreamStreamOuterJoin() {
        
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_NAME);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> leftSource = builder.stream(LEFT_STREAM_TOPIC);
        KStream<String, String> rightSource = builder.stream(RIGHT_STREAM_TOPIC);



        // build the state store that will eventually store all unprocessed items
        Map<String, String> changelogConfig = new HashMap<>();
        // override min.insync.replicas
        //changelogConfig.put("min.insyc.replicas", "1");

        StoreBuilder<KeyValueStore<String, String>> stateStore = Stores.keyValueStoreBuilder(
                                                                    Stores.persistentKeyValueStore(STORE_NAME),
                                                                    Serdes.String(),
                                                                    Serdes.String())
                                                                .withLoggingEnabled(changelogConfig);
                                                                //.withLoggingDisabled();

        // do the outer join 
        // change the value to be a mix of both streams value
        // have a moving window of 5 seconds
        // output the last value received for a specific key during the window
        // push the data to OUTER_JOIN_STREAM_OUT_TOPIC topic        
        leftSource.outerJoin(rightSource,
                            (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, 
                            JoinWindows.of(Duration.ofSeconds(5)))
                        .groupByKey()
                        .reduce(((key, lastValue) -> lastValue))
                        .toStream()
                        .to(OUTER_JOIN_STREAM_OUT_TOPIC);
        
        
        // build the streams topology
        final Topology topology = builder.build();
        
        // add another stream that reads data from OUTER_JOIN_STREAM_OUT_TOPIC topic
        topology.addSource("Source", OUTER_JOIN_STREAM_OUT_TOPIC);

        // add a processor to the stream so that each record is processed
        topology.addProcessor("StateProcessor", 
                            new ProcessorSupplier<String, String>() 
                                    { public Processor<String, String> get() {
                                        return new DataProcessor();
                                    }}, 
                            "Source");
        
        topology.addSink("Sink", PROCESSED_STREAM_OUT_TOPIC, "StateProcessor");
        
        // add a state store so that the processor can push the records which it didnt process
        topology.addStateStore(stateStore, "StateProcessor");

        streamsOuterJoin = new KafkaStreams(topology, props);
        interactiveQueries.setStream(streamsOuterJoin);
        streamsOuterJoin.start();
    }

    @RequestMapping("/storedata")
    public String  getStoreData() {
        return interactiveQueries.getRecords();
    }
}