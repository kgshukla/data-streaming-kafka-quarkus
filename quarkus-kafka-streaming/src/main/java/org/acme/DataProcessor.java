package org.acme;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


public class DataProcessor implements Processor<String, String>{

    private ProcessorContext context;
    private KeyValueStore<String, String> kvStore;
    private final String STORE_NAME = "missing_data_store";
    
	@Override
	public void init(ProcessorContext context) {
        this.context = context;
        kvStore = (KeyValueStore) context.getStateStore(STORE_NAME);

        // schedule a punctuate() method every 10 seconds based on stream-time
        this.context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, 
                                new Punctuator(){
                                
                                    @Override
                                    public void punctuate(long timestamp) {       
                                        System.out.println("Scheduled punctuator called at "+timestamp);                                 
                                        KeyValueIterator<String, String> iter = kvStore.all();
                                        while (iter.hasNext()) {
                                            KeyValue<String, String> entry = iter.next();
                                            System.out.println("  Processed key: "+entry.key+" and value: "+entry.value+" and sending to processed-topic topic");
                                            context.forward(entry.key, entry.value.toString());
                                            kvStore.put(entry.key, null);
                                        }
                                        iter.close();
                            
                                    // commit the current processing progress
                                    context.commit(); 
                                    }
                                }
        );

	}

	@Override
	public void process(String key, String value) {
        if(value.contains("null")) {
            if (kvStore.get(key) != null) {
                // this means that the other value arrived first
                // you have both the values now and can process the record
                String newvalue = value.concat(" ").concat(kvStore.get(key));
                process(key, newvalue);

                // remove the entry from the statestore (if any left or right record came first as an event)
                kvStore.delete(key);

                context.forward(key, newvalue);
            } else {
                // add to state store as either left or right data is missing
                System.out.println("Incomplete value: "+value+" detected. Putting into statestore for later processing");
                kvStore.put(key, value);
            }
        } else {
            processRecord(key, value);

            // remove the entry from the statestore (if any left or right record came first as an event)
            kvStore.delete(key);

            //forward the processed data to processed-topic topic
            context.forward(key, value);
        }
        context.commit();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
    }
    
    private void processRecord (String key, String value) {
        System.out.println("==== Record Processed ==== key: "+key+" and value: "+value);
    }
    
}