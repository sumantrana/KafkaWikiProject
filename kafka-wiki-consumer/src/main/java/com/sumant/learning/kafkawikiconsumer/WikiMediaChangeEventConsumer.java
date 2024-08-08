package com.sumant.learning.kafkawikiconsumer;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class WikiMediaChangeEventConsumer {

    private final String WIKI_CHANGES_TOPIC = "wiki-changes";
    private final String OPENSEARCH_WIKI_INDEX = "wikimedia";

    private final RestHighLevelClient openSearchHighLevelRestClient;

    public WikiMediaChangeEventConsumer(RestHighLevelClient restHighLevelClient) throws IOException {
        this.openSearchHighLevelRestClient = restHighLevelClient;

        //Create wikimedia index to store the messages
        if(! this.openSearchHighLevelRestClient.indices().exists(new GetIndexRequest(OPENSEARCH_WIKI_INDEX), RequestOptions.DEFAULT)){
            this.openSearchHighLevelRestClient.indices().create(new CreateIndexRequest(OPENSEARCH_WIKI_INDEX),
                    RequestOptions.DEFAULT);
            log.info("Created wikimedia index in opensearch");
        } else {
            log.info("Wikimedia index already exists in opensearch");
        }

    }

    @PreDestroy
    public void preDestroy() throws IOException {
        this.openSearchHighLevelRestClient.close();
    }

    @KafkaListener(id = "wikiGroup", topics = WIKI_CHANGES_TOPIC)
    public void consumeWikiMediaMessages(ConsumerRecord<String, String> consumerRecord) {

        log.info(consumerRecord.toString());

        try {
            IndexRequest indexRequest = new IndexRequest(OPENSEARCH_WIKI_INDEX).source(consumerRecord.value(), XContentType.JSON);
            IndexResponse response = this.openSearchHighLevelRestClient.index(indexRequest, RequestOptions.DEFAULT);
            log.info("Inserted one document in OpenSearch with Id: " + response.getId());

        } catch (Exception exp){
            log.error("Exception while processing consumer record", exp);
        }

    }

}
