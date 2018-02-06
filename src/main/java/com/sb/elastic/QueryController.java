package com.sb.elastic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

@Controller
public class QueryController {

    private static final Logger LOG = LoggerFactory.getLogger(QueryController.class);

    private final String INDEX_TPCDS_STORE_SALES = "tpcds_store_sales";
    private final String INDEX_TPCDS_SS_I_S_D = "tpcds_ss_i_s_d"; // i_category truncated
    private final String INDEX_JOIN_DATATYPE_TEST = "sb_join_datatype_test";

    @RequestMapping("/queryLocal")
    @ResponseBody
    public String queryLocal() throws IOException {
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

    @RequestMapping(value = "/queryRemote", produces = "application/json")
    @ResponseBody
    public String queryRemote() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        SearchRequest searchRequest = new SearchRequest("tpc-testdaten-2017.10");
        searchRequest.source(sourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);

        client.close();

        return this.formatResponse(searchResponse);
    }

    @RequestMapping(value = "/queryStoreSales", produces = "application/json")
    @ResponseBody
    public String queryStoreSales() throws IOException {
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
     */
    @RequestMapping(value = "/queryStoreSalesTest", produces = "application/json")
    @ResponseBody
    public String queryStoreSalesTest() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        sourceBuilder.query(boolQuery()
                .must(QueryBuilders.termQuery("i_category.keyword", "Children")) // funktioniert
//                .must(QueryBuilders.matchPhraseQuery("i_category", "Children")) // funktioniert NICHT
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

        SearchRequest searchRequest = new SearchRequest(INDEX_TPCDS_SS_I_S_D);
        searchRequest.source(sourceBuilder);

        LOG.info(searchRequest.toString());

        SearchResponse searchResponse = client.search(searchRequest);

        client.close();

        Aggregations aggregations = searchResponse.getAggregations();

        StringBuilder sb = new StringBuilder();

        Sum sum = aggregations.get("ss_sales_price");
        LOG.info(sum.getValueAsString());

        sb.append(aggregation.toString() + ": " +  sum.getValueAsString());

        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());

        sb.append(this.formatResponse(searchResponse));

        sb.append(System.lineSeparator());
        sb.append(System.lineSeparator());

        sb.append(searchRequest.toString());

        return sb.toString();
    }

    /**
     * Generiert die Dokumente, um den Join-Datatype zu testen.
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/generateJoinDatatypeTestIndex")
    @ResponseBody
    public String generateJoinDatatypeTestIndex() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        // delete existing index
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_JOIN_DATATYPE_TEST);
        try {
            DeleteResponse deleteResponse = client.delete(deleteRequest);
        } catch (ActionRequestValidationException e) {
            LOG.info(e.getMessage());
        }

        // create parent
        Map<String, Object> parent = new HashMap<>();
        parent.put("name","Parent1");
        parent.put("age", 40);
        UUID parentId = UUID.randomUUID();
        IndexRequest indexRequest = new IndexRequest(INDEX_JOIN_DATATYPE_TEST, "parent", parentId.toString())
                .source(parent);

        IndexResponse indexResponse = client.index(indexRequest);

        // create children
        Map<String, Object> child1 = new HashMap<>();
        child1.put("name","Child1");
        child1.put("age", 20);
        child1.put("parentId", parentId.toString());
        indexRequest = new IndexRequest(INDEX_JOIN_DATATYPE_TEST, "child", UUID.randomUUID().toString())
                .source(child1);
        indexRequest.parent("parent");
        indexResponse = client.index(indexRequest);

//        // create children
//        Map<String, Object> child2 = new HashMap<String, Object>();
//        child2.put("name","Child1");
//        child2.put("age", 20);
//        indexResponse = client.index(indexRequest);
//        indexRequest = new IndexRequest(INDEX_JOIN_DATATYPE_TEST, "child", UUID.randomUUID().toString())
//                .source(child2);
//        indexRequest.parent(parentId.toString());
//        indexResponse = client.index(indexRequest);

        client.close();

        String index = indexResponse.getIndex();
        String type = indexResponse.getType();
        String id = indexResponse.getId();
        long version = indexResponse.getVersion();

        String msg = "";
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            msg = indexResponse.getResult().toString();
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            msg = indexResponse.getResult().toString();
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            // Handle the situation where number of successful shards is less than total shards
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
                // Handle the potential failures
            }
        }

        return msg;
    }

    // funktioniert eh nicht, deshalb auskommentiert
//    /**
//     * Generiert die Dokumente, um den Join-Datatype zu testen.
//     * @return
//     * @throws IOException
//     */
//    @RequestMapping(value = "/generateJoinDatatypeTestIndex")
//    @ResponseBody
//    public String generateJoinDatatypeTestIndex() throws IOException {
//        RestHighLevelClient client = this.getRemoteClient();
//
//        // delete existing index
//        DeleteRequest deleteRequest = new DeleteRequest("questions-and-answers");
//        try {
//            DeleteResponse deleteResponse = client.delete(deleteRequest);
//        } catch (ActionRequestValidationException e) {
//            LOG.info(e.getMessage());
//        }
//
//        XContentBuilder builder = jsonBuilder()
//                .startObject()
//                    .startObject("mappings")
//                        .startObject("doc")
//                            .startObject("properties")
//                                .startObject("my_join_field")
//                                    .field("type", "join")
//                                    .startObject("relations")
//                                        .field("question", "answer")
//                                    .endObject()
//                                .endObject()
//                            .endObject()
//                        .endObject()
//                    .endObject()
//                .endObject();
//
//        String json = builder.string();
//
//        Map<String, String> mapping = new HashMap<>();
//        client.getLowLevelClient().performRequest("PUT", "questions-and-answers", json);
//
//        client.close();
//
//        String index = indexResponse.getIndex();
//        String type = indexResponse.getType();
//        String id = indexResponse.getId();
//        long version = indexResponse.getVersion();
//
//        String msg = "";
//        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
//            msg = indexResponse.getResult().toString();
//        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
//            msg = indexResponse.getResult().toString();
//        }
//        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
//        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
//            // Handle the situation where number of successful shards is less than total shards
//        }
//        if (shardInfo.getFailed() > 0) {
//            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
//                String reason = failure.reason();
//                // Handle the potential failures
//            }
//        }
//
//        return msg;
//    }

    private RestHighLevelClient getRemoteClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("vnew.verstehe.local", 3500, "http")));
    }

    private String formatResponse(SearchResponse searchResponse) {
        List<Object> response = new ArrayList<>();
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String sourceAsString = hit.getSourceAsString();
            LOG.info(sourceAsString);
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            response.add(sourceAsMap);
        }

        ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);
        } catch (JsonProcessingException e) {
            return e.getMessage();
        }
    }

}
