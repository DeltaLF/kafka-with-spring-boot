package com.practicekafka.controller;

import java.util.HashMap;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practicekafka.LibraryEventsProducerApplication;
import com.practicekafka.domain.LibraryEvent;
import com.practicekafka.util.TestUtil;

// It's an integration test, so a real API is going to be sent.
// kafka dockers should be up for integration test

// @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@SpringBootTest(classes = LibraryEventsProducerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = "library-events")
// We want to have a kafka server in integration env instead of having a
// separate kafka server
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}" })
// overwrite application.yml for test env
public class LibraryEventsControllerTests {

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() throws Exception {
        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs)
                .createConsumer();
        // won't work while specifying serializer
        // consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs, new
        // IntegerDeserializer(),
        // new StringDeserializer())
        // .createConsumer();
        // this.consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs,
        // IntegerDeserializer::new,
        // StringDeserializer::new)
        // .createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(this.consumer);

    }

    @AfterEach
    void trearDown() {
        this.consumer.close();
    }

    @Test
    void postLibraryEvent() {

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type", MediaType.APPLICATION_JSON.toString());
        var httpEntity = new HttpEntity<>(TestUtil.libraryEventRecord(),
                httpHeaders);
        var responseEntity = restTemplate.exchange("/v1/libraryevent", HttpMethod.POST, httpEntity, LibraryEvent.class);

        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);

        assert consumerRecords.count() == 1;

        consumerRecords.forEach(record -> {
            var libraryEventActual = TestUtil.parseLibraryEventRecord(objectMapper, record.value());
            System.out.print("################\nlibraryEventActual:" + libraryEventActual + "\n################");
            assertEquals(TestUtil.libraryEventRecord(), libraryEventActual);
        });

    }
}
