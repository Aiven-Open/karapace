package poc.schemalinking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * Checks, against a registry that supports contexts, that the standard Confluent Java client:
 *
 * <ol>
 *   <li>with a standard configuration ({@code schema.registry.url} only), sends the subject of the topic on
 *       {@code GET /schemas/ids/{id}};</li>
 *   <li>decodes two topics (default and {@code .A}) whose messages carry the same id, with a single
 *       deserializer, the registry resolving the context from the subject name.</li>
 * </ol>
 *
 * <p>Messages are produced by {@code KafkaAvroSerializer}. The producer of A points to context {@code .A}
 * through its URL: its messages carry the id allocated in that context, as replicated messages carry the
 * id of their origin cluster.
 */
public final class ContextScenario {

  static final String TOPIC_LOCAL = "orders";
  static final String TOPIC_REPLICATED = "A.orders";
  static final String SUBJECT_LOCAL = TOPIC_LOCAL + "-value";
  static final String SUBJECT_REPLICATED = ":.A:" + TOPIC_REPLICATED + "-value";

  // Schema Y: produced locally on B, default context.
  static final Schema SCHEMA_B = new Schema.Parser().parse("""
      {"type": "record", "name": "Order", "namespace": "com.example.b",
       "fields": [{"name": "id", "type": "string"}, {"name": "amount", "type": "double"}]}""");

  // Schema X: produced on A, replicated to B, resolved in the mirror context .A.
  static final Schema SCHEMA_A = new Schema.Parser().parse("""
      {"type": "record", "name": "Order", "namespace": "com.example.a",
       "fields": [{"name": "orderId", "type": "long"}, {"name": "customer", "type": "string"},
                  {"name": "items", "type": "int"}]}""");

  static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

  private static final ObjectMapper JSON = new ObjectMapper();

  private final Properties producerBConfig;
  private final Properties producerAConfig;
  private final Properties consumerConfig;
  private final String bootstrap;
  private final String registryUrl;
  private final String configSource;
  private final List<String> failures = new ArrayList<>();

  ContextScenario(Properties producerBConfig, Properties producerAConfig, Properties consumerConfig, String configSource) {
    this.configSource = configSource;
    this.producerBConfig = producerBConfig;
    this.producerAConfig = producerAConfig;
    this.consumerConfig = consumerConfig;
    this.bootstrap = consumerConfig.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    this.registryUrl = consumerConfig.getProperty(SCHEMA_REGISTRY_URL);
  }

  // consumeOnly: read back the messages already produced, without registering or producing (scenario.consume-only).
  boolean run(boolean consumeOnly) throws Exception {
    System.out.printf("Context scenario: registry %s, Kafka %s%n", registryUrl, bootstrap);
    System.out.println("   configuration: " + configSource);

    GenericRecord orderB = new GenericData.Record(SCHEMA_B);
    orderB.put("id", "B-1");
    orderB.put("amount", 9.99);
    GenericRecord orderA = new GenericData.Record(SCHEMA_A);
    orderA.put("orderId", 42L);
    orderA.put("customer", "alice");
    orderA.put("items", 3);
    Map<TopicPartition, Long> offsets = new HashMap<>();
    if (consumeOnly) {
      section("1. Read only (scenario.consume-only): no schema registered, no message produced");
      lastOffsets(offsets);
    } else {
      // 1. Produce with KafkaAvroSerializer: each producer registers its schema (auto.register.schemas).
      // A failing producer (registry without contexts, for instance) is reported KO; the scenario goes on
      // with what was produced, to show the registry behaviour rather than stop.
      createTopics();
      section("1. Produce with KafkaAvroSerializer");
      produce(producerBConfig, "producer-b", TOPIC_LOCAL, orderB, offsets);
      produce(producerAConfig, "producer-a", TOPIC_REPLICATED, orderA, offsets);
    }

    // 2. Collision: each context has its own counter, both schemas have the same id.
    section("2. Schemas registered by the serializers: each context has its own counter");
    SchemaRegistryClient admin = new CachedSchemaRegistryClient(registryUrl, 100);
    Integer idLocal = registeredId(admin, SUBJECT_LOCAL, "default context", SCHEMA_B);
    Integer idReplicated = registeredId(admin, SUBJECT_REPLICATED, "context .A     ", SCHEMA_A);
    if (idLocal != null && idReplicated != null && !idLocal.equals(idReplicated)) {
      fail("collision not reproduced (ids " + idLocal + " and " + idReplicated
          + "): start again from an empty registry (podman compose down)");
    }

    // 3. A single consumer, a single deserializer, standard configuration: no context option.
    // The client sends the unqualified subject (TopicNameStrategy); the registry finds the context: a
    // subject unknown for this id in the default context is tried in every known context
    // (Confluent: AbstractSchemaRegistry.getSchemaKeyUsingContexts). This requires subject names that are
    // unique across contexts, hence the A.orders naming.
    section("3. Consumer configured by the consumer section, no context option");
    try (RecordingProxy proxy = new RecordingProxy(registryUrl)) {
      Map<String, Object> decoded = consume(offsets, proxy.url());
      if (idLocal != null && decoded.containsKey(TOPIC_LOCAL)) {
        check(TOPIC_LOCAL, idLocal, SUBJECT_LOCAL, proxy.requests(), decoded.get(TOPIC_LOCAL), orderB);
      }
      if (idReplicated != null && decoded.containsKey(TOPIC_REPLICATED)) {
        check(TOPIC_REPLICATED, idReplicated, TOPIC_REPLICATED + "-value", proxy.requests(),
            decoded.get(TOPIC_REPLICATED), orderA);
      } else {
        line("KO %-9s not consumed: no message produced with a schema of context .A", TOPIC_REPLICATED);
      }
    }

    int id = idLocal != null ? idLocal : 1;
    printRegistryView(id);
    return report();
  }

  // Id resolution on the registry side: informative, to compare registries.
  private void printRegistryView(int id) throws IOException, InterruptedException {
    section("4. Registry responses (Confluent: reference; Karapace: to compare)");
    for (String path : List.of(
        "/schemas/ids/" + id,
        "/schemas/ids/" + id + "?subject=" + SUBJECT_REPLICATED,
        "/schemas/ids/" + id + "?subject=" + TOPIC_REPLICATED + "-value",
        "/contexts/.A/schemas/ids/" + id,
        "/contexts")) {
      String encoded = path.replace(":", URLEncoder.encode(":", StandardCharsets.UTF_8));
      HttpResponse<String> response = HttpClient.newHttpClient().send(
          HttpRequest.newBuilder(URI.create(registryUrl + encoded)).GET().build(),
          HttpResponse.BodyHandlers.ofString());
      line("GET %-45s -> %d %s", path, response.statusCode(), summarize(response.body()));
    }
  }

  // Response of /schemas/ids/{id} reduced to the subject and the schema name.
  private static String summarize(String body) {
    try {
      JsonNode json = JSON.readTree(body);
      if (json.has("schema")) {
        return json.path("subject").asText() + ", " + new Schema.Parser().parse(json.get("schema").asText()).getFullName();
      }
    } catch (IOException | RuntimeException e) {
      // Body not JSON or schema unreadable: shown as is.
    }
    return body;
  }

  // Offset of the last message of each topic, the one produced by the last full run.
  private void lastOffsets(Map<TopicPartition, Long> offsets) throws InterruptedException {
    try (Admin kafkaAdmin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      for (String topic : List.of(TOPIC_LOCAL, TOPIC_REPLICATED)) {
        TopicPartition tp = new TopicPartition(topic, 0);
        try {
          long end = kafkaAdmin.listOffsets(Map.of(tp, OffsetSpec.latest())).partitionResult(tp).get().offset();
          if (end == 0) {
            line("KO %-9s empty: run the full scenario first", topic);
            fail(topic + ": no message to read back");
          } else {
            offsets.put(tp, end - 1);
            line("OK %-9s reading back offset %d", topic, end - 1);
          }
        } catch (ExecutionException e) {
          line("KO %-9s %s", topic, describe(e));
          fail(topic + ": topic unreadable (" + describe(e) + ")");
        }
      }
    }
  }

  private void createTopics() throws InterruptedException, ExecutionException {
    try (Admin kafkaAdmin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      for (String topic : List.of(TOPIC_LOCAL, TOPIC_REPLICATED)) {
        try {
          kafkaAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        } catch (ExecutionException e) {
          if (!(e.getCause() instanceof TopicExistsException)) {
            throw e;
          }
        }
      }
    }
  }

  private void produce(Properties config, String file, String topic, GenericRecord value,
                       Map<TopicPartition, Long> offsets) {
    String url = config.getProperty(SCHEMA_REGISTRY_URL);
    try (KafkaProducer<String, Object> producer = new KafkaProducer<>(config)) {
      RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, value)).get();
      offsets.put(new TopicPartition(metadata.topic(), metadata.partition()), metadata.offset());
      line("OK %-9s %s, %s -> %s", topic, file, url, value);
    } catch (Exception e) {
      line("KO %-9s %s, %s -> %s", topic, file, url, describe(e));
      fail(topic + ": cannot produce with " + url + " (" + describe(e) + ")");
    }
  }

  // Id of the latest version of the subject, or null if the registry does not know it.
  private Integer registeredId(SchemaRegistryClient admin, String subject, String context, Schema schema) {
    try {
      int id = admin.getLatestSchemaMetadata(subject).getId();
      line("%-20s %s -> ID %d, %s", subject, context, id, schema.getFullName());
      return id;
    } catch (Exception e) {
      line("%-20s %s -> %s", subject, context, describe(e));
      fail(subject + " missing from the registry");
      return null;
    }
  }

  // Reads exactly the message produced at each offset, even if the topics hold earlier runs.
  // Only change to the consumer section: schema.registry.url points to the recording proxy.
  private Map<String, Object> consume(Map<TopicPartition, Long> offsets, String proxyUrl) {
    Properties props = new Properties();
    props.putAll(consumerConfig);
    props.put(SCHEMA_REGISTRY_URL, proxyUrl);
    Map<String, Object> decoded = new HashMap<>();
    try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {
      consumer.assign(offsets.keySet());
      offsets.forEach(consumer::seek);
      long deadline = System.currentTimeMillis() + 30_000;
      while (decoded.size() < offsets.size() && System.currentTimeMillis() < deadline) {
        try {
          for (ConsumerRecord<String, Object> record : consumer.poll(Duration.ofMillis(500))) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            if (record.offset() == offsets.get(tp)) {
              decoded.putIfAbsent(record.topic(), record.value());
            }
          }
        } catch (RecordDeserializationException e) {
          // The unreadable message is recorded as the result of its topic, then skipped.
          decoded.putIfAbsent(e.topicPartition().topic(), e);
          consumer.seek(e.topicPartition(), e.offset() + 1);
        }
      }
    }
    return decoded;
  }

  // One line per topic: request sent by the deserializer, decoded schema, verdict.
  private void check(String topic, int id, String expectedSubject, List<String> requests, Object value,
                     GenericRecord expected) {
    String sent = sentSubject(requests, id, topic + "-value");
    boolean subjectOk = expectedSubject.equals(sent);
    boolean decodedOk = value instanceof GenericRecord r && r.getSchema().equals(expected.getSchema())
        && r.equals(expected);
    line("%s %-9s GET /schemas/ids/%d?subject=%s -> %s", subjectOk && decodedOk ? "OK" : "KO", topic, id, sent,
        describe(value));
    if (!subjectOk) {
      fail(topic + ": subject sent " + sent + ", expected " + expectedSubject);
    }
    if (!decodedOk) {
      fail(topic + " wrongly decoded, expected " + expected.getSchema().getFullName() + " " + expected);
    }
  }

  // The subject sent on GET /schemas/ids/{id} for this topic; the client adds other parameters
  // (fetchMaxId...), only "subject" matters.
  private static String sentSubject(List<String> requests, int id, String unqualifiedSubject) {
    String path = "GET /schemas/ids/" + id + "?";
    return requests.stream()
        .filter(r -> r.startsWith(path))
        .flatMap(r -> Arrays.stream(r.substring(path.length()).split("&")))
        .filter(p -> p.startsWith("subject="))
        .map(p -> p.substring("subject=".length()))
        .filter(subject -> unqualified(subject).equals(unqualifiedSubject))
        .findFirst()
        .orElse("(no request)");
  }

  // ":.A:A.orders-value" -> "A.orders-value".
  private static String unqualified(String subject) {
    return subject.startsWith(":") ? subject.substring(subject.indexOf(':', 1) + 1) : subject;
  }

  private static void section(String title) {
    System.out.println();
    System.out.println(title);
  }

  private static void line(String format, Object... args) {
    System.out.println("   " + String.format(format, args));
  }

  private static String describe(Object value) {
    if (value instanceof GenericRecord r) {
      return r.getSchema().getFullName();
    }
    if (value instanceof Throwable t) {
      // Client errors often wrap the registry response: keep the innermost one.
      Throwable root = t;
      while (root.getCause() != null && root.getCause() != root) {
        root = root.getCause();
      }
      return "error " + root.getClass().getSimpleName() + ": " + root.getMessage();
    }
    return String.valueOf(value);
  }

  private void fail(String message) {
    failures.add(message);
  }

  private boolean report() {
    System.out.println();
    if (failures.isEmpty()) {
      System.out.println("Result: SUCCESS, both topics are decoded correctly with an unpatched client.");
      return true;
    }
    System.out.println("Result: FAILURE, expected behaviour not observed:");
    failures.forEach(f -> System.out.println("  - " + f));
    return false;
  }
}
