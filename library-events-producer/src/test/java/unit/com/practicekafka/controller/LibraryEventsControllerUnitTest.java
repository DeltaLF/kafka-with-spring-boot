package com.practicekafka.controller;

import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practicekafka.domain.LibraryEvent;
import com.practicekafka.producer.LibraryEventsProducer;
import com.practicekafka.util.TestUtil;

//@SpringBootTest -> to bring up the spring boot app in test which is not needed in unit tests
@WebMvcTest(LibraryEventsController.class)
public class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {
        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());
        when(libraryEventsProducer.sendLibraryEventProducerRecord(isA(LibraryEvent.class)))
                .thenReturn(null);
        mockMvc
                .perform(
                        MockMvcRequestBuilders.post("/v1/libraryevent")
                                .content(json)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    @Test
    void postLibraryEvent_withInvalidValues() throws Exception {
        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());
        when(libraryEventsProducer.sendLibraryEventProducerRecord(isA(LibraryEvent.class)))
                .thenReturn(null);
        mockMvc
                .perform(
                        MockMvcRequestBuilders.post("/v1/libraryevent")
                                .content(json)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError());
    }

}
