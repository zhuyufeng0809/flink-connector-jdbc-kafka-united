package oh.awesome.flink.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

public class KafkaMessageSourceOptions {
    public static final ConfigOption<List<String>> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Topic names from which the table is read. Either 'topic' or 'topic-pattern' must be set for source. "
                                    + "Option 'topic' is required for sink.");

    public static final ConfigOption<String> TOPIC_PATTERN =
            ConfigOptions.key("topic-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional topic pattern from which the table is read for source. Either 'topic' or 'topic-pattern' must be set.");

    public static final ConfigOption<String> PROPS_BOOTSTRAP_SERVERS =
            ConfigOptions.key("properties.bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required Kafka server connection string");

    public static final ConfigOption<String> PROPS_GROUP_ID =
            ConfigOptions.key("properties.group.id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Required consumer group in Kafka consumer, no need for Kafka producer");

    public static final ConfigOption<String> VALUE_FORMAT =
            ConfigOptions.key("value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");

}
