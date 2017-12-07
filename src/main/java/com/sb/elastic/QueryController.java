package com.sb.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
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

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

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

    /**
     * Übersetzung des folgenden Querys:
     * SELECT SUM("ss_sales_price") as "ss_sales_price" FROM
     (SELECT store."S_STORE_NAME" AS "Storename", item."I_CATEGORY" AS "Category", sum(store_sales."SS_SALES_PRICE") AS "ss_sales_price"
     FROM S320.store_sales as store_sales, S320.item as item, S320.store as store, S320.date_dim as date_dim
     WHERE (store."S_STORE_NAME"  =   'ought' AND item."I_CATEGORY"  = 'Children'
     AND store_sales."SS_ITEM_SK" = item."I_ITEM_SK" AND store_sales."SS_STORE_SK" = store."S_STORE_SK"
     AND date_dim."D_MOY"  =  '11' AND date_dim."D_YEAR"  =  '2017'
     AND store_sales."SS_SOLD_DATE_SK" = date_dim."D_DATE_SK")
     GROUP BY item.I_CATEGORY, store.S_STORE_NAME
     ORDER BY "Storename", "Category")
     * @return
     * @throws IOException
     */
    @RequestMapping("/queryStoreSalesTest")
    @ResponseBody
    public List queryStoreSalesTest() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(boolQuery()
//                .must(QueryBuilders.termQuery("i_category", "Children"))
                .must(QueryBuilders.termQuery("s_store_name","ought"))
                .must(QueryBuilders.termQuery("d_year", 2017))
                .must(QueryBuilders.termQuery("d_moy", 11))
        );

        String[] includeFields = new String[] {"s_store_name", "i_category", "ss_sales_price"};
        String[] excludeFields = new String[] {};
        sourceBuilder.fetchSource(includeFields, excludeFields);

        SumAggregationBuilder aggregation = AggregationBuilders.sum("ss_sales_price")
                .field("ss_sales_price");
        sourceBuilder.aggregation(aggregation);

        SearchRequest searchRequest = new SearchRequest("tpcds_store_sales");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        Aggregations aggregations = searchResponse.getAggregations();

        Sum sum = aggregations.get("ss_sales_price");
        log.info(sum.getValueAsString());

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
