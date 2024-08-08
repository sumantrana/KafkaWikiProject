package com.sumant.learning.kafkawikiproducer;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaWikiProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaWikiProducerApplication.class, args);
    }

    @Bean
    public ApplicationRunner applicationRunner(WikimediaChangeEventSource wikimediaChangeEventSource) {

        return args -> {
            wikimediaChangeEventSource.start();
            wikimediaChangeEventSource.processMessages();
        };
    }
}
