package com.practicekafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practicekafka.entity.Book;
import com.practicekafka.entity.LibraryEvent;
import com.practicekafka.entity.LibraryEventType;
import com.practicekafka.jpa.LibraryEventsRepository;
import com.practicekafka.service.LibraryEventsService;

@SpringBootTest
@EmbeddedKafka(topics = { "library-events", "library-events.RETRY", "library-events.DLT" }, partitions = 3)
// We want to have a kafka server in integration env instead of having a
// separate kafka server
@TestPropertySource(properties = { "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retryListener.startup=false"
})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;
    // to get all the listener containers

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    private Consumer<Integer, String> consumer;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @BeforeEach
    void setUp() {

        // only want to test consumer (but not retry consumer)
        var container = endpointRegistry.getListenerContainers()
                .stream()
                .filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(),
                        "library-events-listener-group"))
                .collect(Collectors.toList()).get(0);
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        // for (MessageListenerContainer messageListenerContainer :
        // endpointRegistry.getListenerContainers()) {
        // ContainerTestUtils.waitForAssignment(messageListenerContainer,
        // embeddedKafkaBroker.getPartitionsPerTopic());
        // }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent() throws InterruptedException, ExecutionException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\": \"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Hello world\",\"bookAuthor\":\"Kafka\"}}";
        kafkaTemplate.sendDefault(json).get(); // send to default topic (defined in resources/application.yml)

        CountDownLatch latch = new CountDownLatch(1);
        // to make sure sendDefault is completed
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        List<LibraryEvent> libraryEventList = (List<LibraryEvent>) libraryEventsRepository.findAll();
        assert libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(libraryEvent.getBook().getBookId(), 123);
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws InterruptedException, ExecutionException, JsonProcessingException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\": \"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Hello world\",\"bookAuthor\":\"Kafka\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder().bookId(123).bookName("Hello world v2").bookAuthor("fakfa").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        LibraryEvent persistedLibraryEvent = libraryEventsRepository
                .findById(libraryEvent.getLibraryEventId()).get();
        assertEquals(persistedLibraryEvent.getBook().getBookName(), "Hello world v2");
        assertEquals(persistedLibraryEvent.getBook().getBookAuthor(), "fakfa");
    }

    @Test
    void publishUpdateLibraryEvent_null_LibraryEvent()
            throws InterruptedException, ExecutionException, JsonProcessingException {
        String json = "{\"libraryEventId\": 555,\"libraryEventType\": \"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Hello world\",\"bookAuthor\":\"Kafka\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        // because the eventId is not provided and consumer will retry it
        // the retry fixBackOff is defined in LibraryEventsConsumerConfig
        // change to 1 because it's pointless to retry for IllegalArgumentsException
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishUpdateLibraryEvent_9999_LibraryEvent()
            throws InterruptedException, ExecutionException, JsonProcessingException {
        String json = "{\"libraryEventId\":9999,\"libraryEventType\": \"UPDATE \",\"book\":{\"bookId\":123,\"bookName\":\"Hello world\",\"bookAuthor\":\"Kafka\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await(5, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));

        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumer = new DefaultKafkaConsumerFactory<Integer, String>(configs)
                .createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, retryTopic);

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, retryTopic);
        System.out.println("### consumerRecord : " + consumerRecord.value());
        assertEquals(consumerRecord.value(), json);
    }
}
