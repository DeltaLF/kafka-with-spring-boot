package com.practicekafka;

import java.util.Arrays;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

@SpringBootApplication
public class LibraryEventsProducerApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(LibraryEventsProducerApplication.class, args);
		Environment environment = context.getBean(Environment.class);
		System.out.println(
				"####################\nActive profiles: " + Arrays.toString(environment.getActiveProfiles())
						+ "\n####################");
	}

}
