package com.sumant.learning.kafkawikiproducer;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.MalformedURLException;
import java.net.URI;


@Component
@Slf4j
public class WikimediaChangeEventSource {

    private final String WIKI_CHANGE_URI_STRING = "https://stream.wikimedia.org/v2/stream/recentchange";
    private final WikiMediaChangeEventProducer wikiMediaChangeEventProducer;
    private final EventSource eventSource;

    public WikimediaChangeEventSource(WikiMediaChangeEventProducer wikiMediaChangeEventProducer) throws MalformedURLException {
        this.wikiMediaChangeEventProducer = wikiMediaChangeEventProducer;
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(WIKI_CHANGE_URI_STRING));
        this.eventSource = eventSourceBuilder.build();
    }

    public void start() throws StreamException {
        System.out.println("Starting event source");
        this.eventSource.start();
    }

    @PreDestroy
    public void stop(){
        System.out.println("Stopping Event Source");
        this.eventSource.close();
    }

    public void processMessages(){
        while (true){
            this.eventSource.messages().forEach(message -> wikiMediaChangeEventProducer.sendMessage(message.getData()));
        }
    }

}
