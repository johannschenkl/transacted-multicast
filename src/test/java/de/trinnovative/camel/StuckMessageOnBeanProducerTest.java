package de.trinnovative.camel;

import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.processor.aggregate.GroupedBodyAggregationStrategy;
import org.apache.camel.spring.javaconfig.SingleRouteCamelConfiguration;
import org.apache.camel.test.spring.junit5.CamelSpringTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;

import java.util.concurrent.TimeUnit;

@CamelSpringTest
@ContextConfiguration(classes = {App.class, StuckMessageOnBeanProducerTest.ContextConfig.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class StuckMessageOnBeanProducerTest {

    @EndpointInject("mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce("direct:start")
    protected ProducerTemplate template;

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    public void testSendMatchingMessage() throws Exception {
        String expectedBody = "[{\"id\":3}]";

        resultEndpoint.expectedBodiesReceived(expectedBody);

        template.sendBody("");

        resultEndpoint.assertIsSatisfied();
    }

    @Configuration
    public static class ContextConfig extends SingleRouteCamelConfiguration {

        public static class Sender {

            @EndpointInject("direct:multicast")
            private ProducerTemplate producer;

            public void handle(Exchange ex) {
                producer.sendBody("[{\"id\":3}]");
            }
        }

        @Bean
        public Sender sender() {
            return new Sender();
        }

        @Override
        @Bean
        public RouteBuilder route() {
            return new RouteBuilder() {
                public void configure() {

                    from("direct:start").transacted().bean(sender());

                    from("direct:multicast")
                            .unmarshal().json()
                            .multicast()
                            .to("direct:split");

                    from("direct:split")
                            .split(body())
                            .to("direct:aggregate");

                    from("direct:aggregate")
                            .aggregate(simple("${body['id']}"))
                            .completionSize(1).aggregationStrategy(new GroupedBodyAggregationStrategy())
                            .marshal().json()
                            .to("mock:result");
                }
            };
        }
    }
}