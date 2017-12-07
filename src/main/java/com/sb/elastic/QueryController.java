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

    @RequestMapping("/queryLocal")
    @ResponseBody
    public List query() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery("ss_store_sk", 1));

        SearchRequest searchRequest = new SearchRequest("tpcds_mini");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        client.close();

        return this.formatResponse(searchResponse);
    }

    @RequestMapping("/queryRemote")
    @ResponseBody
    public List queryRemote() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        SearchRequest searchRequest = new SearchRequest("tpc-testdaten-2017.10");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        client.close();

        return this.formatResponse(searchResponse);
    }

    @RequestMapping("/queryStoreSales")
    @ResponseBody
    public List queryStoreSales() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        SearchRequest searchRequest = new SearchRequest("tpcds_store_sales");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        client.close();

        return this.formatResponse(searchResponse);
    }

    @RequestMapping("/queryStoreSalesTest")
    @ResponseBody
    public List queryStoreSalesTest() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(QueryBuilders.termQuery("s_store_name  ", "ought"));
        sourceBuilder.query(QueryBuilders.termQuery("d_moy", 11));
        sourceBuilder.query(QueryBuilders.termQuery("d_year", 2017));

        String[] includeFields = new String[] {"s_store_name", "i_category", "ss_sales_price"};
        String[] excludeFields = new String[] {};
        sourceBuilder.fetchSource(includeFields, excludeFields);

        SearchRequest searchRequest = new SearchRequest("tpcds_store_sales");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        client.close();

        return this.formatResponse(searchResponse);
    }

    private RestHighLevelClient getRemoteClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("vnew.verstehe.local", 3500, "http")));
        return client;
    }

    private List formatResponse(SearchResponse searchResponse) {
        List<Object> response = new ArrayList<>();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String sourceAsString = hit.getSourceAsString();
            log.info(sourceAsString);
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            response.add(sourceAsMap);
        }
        return response;
    }

}
