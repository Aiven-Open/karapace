package poc.schemalinking;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.yaml.snakeyaml.Yaml;

/**
 * Entry point: reads client.yaml, from the classpath or from the file given by {@code -Dscenario.config=...},
 * then runs the scenario. The logging section is applied before loading {@link ContextScenario}, whose Avro
 * schemas create the first loggers: slf4j-simple reads its configuration only once.
 *
 * <p>{@code -Dscenario.consume-only=true|false} overrides {@code scenario.consume-only} without editing the file.
 */
public final class Main {

  static final String CONFIG_PROPERTY = "scenario.config";
  static final String CONSUME_ONLY_PROPERTY = "scenario.consume-only";

  public static void main(String[] args) throws Exception {
    String file = System.getProperty(CONFIG_PROPERTY);
    Map<String, Object> config = load(file);
    section(config, "logging").forEach((key, value) -> System.setProperty((String) key, (String) value));

    String consumeOnly = System.getProperty(CONSUME_ONLY_PROPERTY,
        section(config, "scenario").getProperty("consume-only", "false"));
    ContextScenario scenario = new ContextScenario(section(config, "producer-b"), section(config, "producer-a"),
        section(config, "consumer"), file != null ? file : "classpath:client.yaml");
    System.exit(scenario.run(Boolean.parseBoolean(consumeOnly)) ? 0 : 1);
  }

  private static Map<String, Object> load(String file) throws IOException {
    try (InputStream in = file != null
        ? Files.newInputStream(Path.of(file))
        : Main.class.getResourceAsStream("/client.yaml")) {
      if (in == null) {
        throw new IOException("configuration not found: client.yaml");
      }
      Map<String, Object> config = new Yaml().load(in);
      if (config == null) {
        throw new IOException("empty configuration: " + (file != null ? file : "client.yaml"));
      }
      return config;
    }
  }

  // A YAML section, flattened, values converted to strings as in a properties file.
  static Properties section(Map<String, Object> config, String name) throws IOException {
    Object section = config.get(name);
    if (!(section instanceof Map<?, ?> map)) {
      throw new IOException("missing or invalid section in client.yaml: " + name);
    }
    Properties props = new Properties();
    map.forEach((key, value) -> props.setProperty(String.valueOf(key), String.valueOf(value)));
    return props;
  }
}
