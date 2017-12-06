package com.sb.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Controller
public class QueryController {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @RequestMapping("/query")
    @ResponseBody
    public List query() {
        List<Object> response = new ArrayList<>();

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        try {

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.termQuery("ss_store_sk", 1));

            SearchRequest searchRequest = new SearchRequest("tpcds_mini");
            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest);

            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                String sourceAsString = hit.getSourceAsString();
                log.info(sourceAsString);
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                response.add(sourceAsMap);
            }

            client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return response;
    }

    @RequestMapping("/queryRemote")
    @ResponseBody
    public List queryRemote() {
        List<Object> response = new ArrayList<>();

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("vnew.verstehe.local", 3500, "http")));

        try {

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            SearchRequest searchRequest = new SearchRequest("tpc-testdaten-2017.10");
            searchRequest.source(sourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest);

            SearchHits hits = searchResponse.getHits();
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                String sourceAsString = hit.getSourceAsString();
                log.info(sourceAsString);
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                response.add(sourceAsMap);
            }

            client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return response;
    }

}
