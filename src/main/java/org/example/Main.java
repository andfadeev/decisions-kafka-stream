package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerializer;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.*;

import org.slf4j.Logger;

// OUTPUT OF THE PROGRAM:

//Sending events, event-1, event-1
//    Sending events, event-2, event-2
//    Sending decisions, event-1, event-1-ok
//MERGED: event-1: Event: event-1, Decision: event-1-ok
//MERGED: event-2: Event: event-2, Decision: DEFAULT
public class Main {

  public static class EventProcessor implements Processor<String, String, String, String> {
    private KeyValueStore<String, ValueAndTimestamp<String>> stateStore;
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
      this.context = context;
      this.stateStore = context.getStateStore(STATE_STORE);
    }

    public void process(Record<String, String> record) {

//      System.out.println( "PROCESS:" + record.key() + ": " + record.value());
      long currentTime = context.currentSystemTimeMs();

      stateStore.put(record.key(), ValueAndTimestamp.make(record.value(), currentTime));
      context.schedule(Duration.ofSeconds(10), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
//        System.out.println("IN punctuator:" + record.key() + " " + timestamp);
        ValueAndTimestamp<String> event = stateStore.get(record.key());
        if (event != null && (timestamp - event.timestamp()) >= Duration.ofSeconds(10).toMillis()) {
          String result = String.format("Event: %s, Decision: DEFAULT", event.value());
          context.forward(new Record<>(record.key(), result, timestamp));
          stateStore.delete(record.key());
        }
      });
    }
  }

  public static class DecisionProcessor implements Processor<String, String, String, String> {
    private KeyValueStore<String, ValueAndTimestamp<String>> stateStore;
    private ProcessorContext<String, String> context;

    @Override
    public void init(ProcessorContext<String, String> context) {
      this.context = context;
      this.stateStore = context.getStateStore(STATE_STORE);
    }

    public void process(Record<String, String> record) {
      ValueAndTimestamp<String> event = stateStore.get(record.key());
      if (event != null) {
        String result = String.format("Event: %s, Decision: %s", event.value(), record.value());
        context.forward(new Record<>(record.key(), result, record.timestamp()));
        stateStore.delete(record.key());
      }
    }
  }

  private static final String EVENTS_TOPIC = "events";
  private static final String DECISIONS_TOPIC = "decisions";
  private static final String OUTPUT_TOPIC = "output";
  private static final String STATE_STORE = "state-store";

  public static Logger logger = LoggerFactory.getLogger(Main.class);

  private static void createTopics(String kafkaBootstrapServers, String... topics) {
    var newTopics =
        Arrays.stream(topics)
            .map(topic -> new NewTopic(topic, 3, (short) 1))
            .collect(Collectors.toList());
    try (var admin = AdminClient.create(Map.of("bootstrap.servers", kafkaBootstrapServers))) {
      admin.createTopics(newTopics);
    }
  }

  public static void publishMessage(Producer<String, String> producer,
                                    String topic,
                                    String key,
                                    String value) throws ExecutionException, InterruptedException {
    System.out.println(String.format("Sending %s, %s, %s", topic, key, value));
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
    producer.send(record).get();
  }

  public static Producer<String, String> createKafkaProducer(String kafkaBootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaBootstrapServers);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaProducer<>(props);
  }

  public static void startKafkaStream(String kafkaBootstrapServers) {

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> events = builder.stream(EVENTS_TOPIC);
    KStream<String, String> decisions = builder.stream(DECISIONS_TOPIC);


    builder.addStateStore(Stores.timestampedKeyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(STATE_STORE),
        Serdes.String(),
        Serdes.String()
        )
    );

    KStream<String, String> processedEvents = events.process(EventProcessor::new, STATE_STORE);
    KStream<String, String> processedDecisions = decisions.process(DecisionProcessor::new, STATE_STORE);

    KStream<String, String> merged = processedEvents.merge(processedDecisions);

    merged.peek((k, v) -> System.out.println("MERGED: " + k + ": " + v));

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-decision-joiner");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();
  }


  public static void main(String[] args) throws InterruptedException, ExecutionException {
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse(
        "confluentinc/cp-kafka:7.4.0"))
        .withKraft();
    kafka.start();

    createTopics(kafka.getBootstrapServers(),
        EVENTS_TOPIC, DECISIONS_TOPIC, OUTPUT_TOPIC);

    Producer<String, String> kafkaProducer = createKafkaProducer(kafka.getBootstrapServers());

    logger.info("Starting Kafka Stream");

    startKafkaStream(kafka.getBootstrapServers());
    Thread.sleep(1000);
    publishMessage(kafkaProducer, EVENTS_TOPIC, "event-1", "event-1");
    publishMessage(kafkaProducer, EVENTS_TOPIC, "event-2", "event-2");
//        publishMessage(kafkaProducer, "trigger-events", "trigger-event-3", "trigger-event-3");
    Thread.sleep(5000);
    publishMessage(kafkaProducer, DECISIONS_TOPIC, "event-1", "event-1-ok");
//        publishMessage(kafkaProducer, "decision-events", "trigger-event-1", "decision-ok");
//        publishMessage(kafkaProducer, "decision-events", "trigger-event-1", "decision-ok");

//        publishMessage(kafkaProducer, "decision-events", "trigger-event-2", "decision-not-ok");
//        Thread.sleep(10000);
//
//        publishMessage(kafkaProducer, "decision-events", "trigger-event-3", "decision-LATE");

    Thread.sleep(20000);
    System.out.println("Done!");
  }
}
