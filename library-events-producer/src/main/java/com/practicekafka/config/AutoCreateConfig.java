package com.practicekafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local") // because for ssl env, there is no enough broker
public class AutoCreateConfig {

    @Value("${spring.kafka.topic}") // from resources/application.yml
    public String topic;

    @Bean
    public NewTopic libraryEvents() {

        return TopicBuilder
                .name(topic)
                .partitions(3)
                .replicas(3)
                .build();
    }
}
