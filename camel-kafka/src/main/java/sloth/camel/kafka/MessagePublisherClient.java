package sloth.camel.kafka;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagePublisherClient {

    private static final Logger LOG = LoggerFactory.getLogger(MessagePublisherClient.class);

    private MessagePublisherClient() {
    }

    public static void main(String[] args) throws Exception {

        LOG.info("About to run Kafka-camel integration...");

        String testKafkaMessage = "Test Message from  MessagePublisherClient " + Calendar.getInstance().getTime();

        CamelContext camelContext = new DefaultCamelContext();

        // Add route to send messages to Kafka

        camelContext.addRoutes(new RouteBuilder() {
            public void configure() {
                PropertiesComponent pc = getContext().getComponent("properties", PropertiesComponent.class);
                pc.setLocation("classpath:application.properties");

                // setup kafka component with the brokers
                KafkaComponent kafka = new KafkaComponent();
                kafka.setBrokers("{{kafka.host}}:{{kafka.port}}");
                camelContext.addComponent("kafka", kafka);

                from("direct:kafkaStart").routeId("DirectToKafka").to("kafka:{{producer.topic}}").log("${headers}");

                // Topic can be set in header as well.

                from("direct:kafkaStartNoTopic").routeId("kafkaStartNoTopic").to("kafka:dummy").log("${headers}");

                // Use custom partitioner based on the key.

                from("direct:kafkaStartWithPartitioner").routeId("kafkaStartWithPartitioner").to("kafka:{{producer.topic}}?partitioner={{producer.partitioner}}").log("${headers}");

                // Takes input from the command line.

                from("stream:in").setHeader(KafkaConstants.PARTITION_KEY, simple("0")).setHeader(KafkaConstants.KEY, simple("1")).to("direct:kafkaStart");

            }

        });

        ProducerTemplate producerTemplate = camelContext.createProducerTemplate();
        camelContext.start();

        Map<String, Object> headers = new HashMap<String, Object>();

        headers.put(KafkaConstants.PARTITION_KEY, 0);
        headers.put(KafkaConstants.KEY, "1");
        producerTemplate.sendBodyAndHeaders("direct:kafkaStart", testKafkaMessage, headers);

        // Send with topicName in header

        testKafkaMessage = "TOPIC " + testKafkaMessage;
        headers.put(KafkaConstants.KEY, "2");
        headers.put(KafkaConstants.TOPIC, "TestLog");

        producerTemplate.sendBodyAndHeaders("direct:kafkaStartNoTopic", testKafkaMessage, headers);

        testKafkaMessage = "PART 0 :  " + testKafkaMessage;
        Map<String, Object> newHeader = new HashMap<String, Object>();
        newHeader.put(KafkaConstants.KEY, "AB"); // This should go to partition 0

        producerTemplate.sendBodyAndHeaders("direct:kafkaStartWithPartitioner", testKafkaMessage, newHeader);

        testKafkaMessage = "PART 1 :  " + testKafkaMessage;
        newHeader.put(KafkaConstants.KEY, "ABC"); // This should go to partition 1

        producerTemplate.sendBodyAndHeaders("direct:kafkaStartWithPartitioner", testKafkaMessage, newHeader);

        LOG.info("Successfully published event to Kafka.");
        System.out.println("Enter text on the line below : [Press Ctrl-C to exit.] ");

        Thread.sleep(5 * 60 * 1000);

        camelContext.stop();
    }
}