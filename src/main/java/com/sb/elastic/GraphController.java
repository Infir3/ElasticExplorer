package com.sb.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sb.elastic.model.Measure;
import com.sb.elastic.model.Report;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.elasticsearch.xpack.core.graph.action.GraphExploreAction;
import org.elasticsearch.xpack.core.graph.action.GraphExploreRequestBuilder;
import org.elasticsearch.xpack.core.graph.action.GraphExploreResponse;
import org.elasticsearch.xpack.core.graph.action.Hop;
import org.elasticsearch.xpack.core.graph.action.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class GraphController {

    private static final Logger LOG = LoggerFactory.getLogger(GraphController.class);

    private static final String INDEX_REPORTING_DATA = "reporting_data";

    @RequestMapping("/createReportingData")
    @ResponseBody
    public void createReportingData() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(INDEX_REPORTING_DATA);
            DeleteIndexResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest);
        } catch (ElasticsearchStatusException e) {
            LOG.error(e.getMessage(), e);
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_REPORTING_DATA);
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest);

//        List<Report> reportList = this.createReportList();
        List<Report> reportList = this.createReportListRandomized();

        // instance a json mapper
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse

        BulkRequest request = new BulkRequest();
        for (Report report : reportList) {
            String stringifiedJson = mapper.writeValueAsString(report);
            request.add(new IndexRequest(INDEX_REPORTING_DATA, "report")
                .source(stringifiedJson, XContentType.JSON));
        }

        // this provokes an ElasticsearchException because more than 1 type per
        // mapping is not allowed
//        for (Measure measure : this.createMeasureList()) {
//            String stringifiedJson = mapper.writeValueAsString(measure);
//            request.add(new IndexRequest(INDEX_REPORTING_DATA, "measure")
//                .source(stringifiedJson, XContentType.JSON));
//        }

        BulkResponse bulkResponse = client.bulk(request);
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                LOG.error(failure.getMessage());
            }
        }

        client.close();
    }

    private List<Report> createReportList() {
        List<Report> reportList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String title = "temp" + i;
            String description = "description";
            Report report = new Report(title, description);
            reportList.add(report);
        }

        reportList.get(0).getMeasures().add(1);
        reportList.get(0).getMeasures().add(2);
        reportList.get(0).getMeasures().add(3);

        reportList.get(1).getMeasures().add(1);
        reportList.get(1).getMeasures().add(3);

        reportList.get(2).getMeasures().add(3);

        reportList.get(3).getMeasures().add(3);

        reportList.get(4).getMeasures().add(3);

        reportList.get(5).getMeasures().add(3);
        return reportList;
    }

    /**
     * Erstellt eine Liste an Reports, die jeweils n Kennzahlen mit einer Id m enthalten. M und n
     * sind dabei beide binomial verteilt.
     */
    private List<Report> createReportListRandomized() {
        List<Report> reportList = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            String title = "temp" + i;
            String description = "description" + i;
            Report report = new Report(title, description);
            int numberOfMeasures = this.getBinomial(10, 0.5);
            for (int j = 0; j < numberOfMeasures; j++) {
                int measure = this.getBinomial(10, 0.5);
                report.getMeasures().add(measure);
            }
            reportList.add(report);
        }

        return reportList;
    }

    @RequestMapping("/queryReportingData")
    @ResponseBody
    public void queryReportingData() {
        TransportClient client = new PreBuiltXPackTransportClient(Settings.EMPTY)
            .addTransportAddress(new TransportAddress(new InetSocketAddress("localhost", 9300)));

        int requiredNumberOfSightings = 1;

        GraphExploreRequestBuilder grb = new GraphExploreRequestBuilder(client,
            GraphExploreAction.INSTANCE).setIndices(INDEX_REPORTING_DATA);

        Hop hopNew = grb.createNextHop(QueryBuilders.termQuery("_all", "*"));
        hopNew.addVertexRequest("title.keyword").size(10).minDocCount(requiredNumberOfSightings);
        GraphExploreResponse response = grb.get();

        Hop hop1 = grb.createNextHop(QueryBuilders.termQuery("_all", "elasticsearch"));
        // elasticsearch meetup attendees
        hop1.addVertexRequest("member_id").size(10).minDocCount(requiredNumberOfSightings);
        // groups attended by elastic attendees
        grb.createNextHop(null).addVertexRequest("group_id").size(10)
            .minDocCount(requiredNumberOfSightings);

        response = grb.get();
        Collection<Vertex> vertices = response.getVertices();

        System.out.println("==Members===");
        for (Vertex vertex : vertices) {
            if (vertex.getField().equals("member_id")) {
                System.out.println(vertex.getTerm());
            }
        }

        System.out.println("==Groups===");
        for (Vertex vertex : vertices) {
            if (vertex.getField().equals("group_id")) {
                System.out.println(vertex.getTerm());
            }
        }

        client.close();
    }

    @RequestMapping("/queryReportingDataRest")
    @ResponseBody
    public void queryReportingDataRest() throws IOException {
        RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200, "http"),
            new HttpHost("localhost", 9201, "http")).build();
        Response response = restClient.performRequest("GET", "/");
        String responseBody = EntityUtils.toString(response.getEntity());

    }

    private RestHighLevelClient getRemoteClient() {
        return new RestHighLevelClient(
            RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
    }

    private List<Measure> createMeasureList() {
        List<Measure> measureList = new ArrayList<>();
        measureList.add(new Measure(1, "tolle Kennzahl", "Beschreibung der tollen Kennzahl"));
        measureList
            .add(new Measure(2, "langweilige Kennzahl", "Beschreibung der langweilige Kennzahl"));
        measureList.add(new Measure(3, "dumme Kennzahl", "Beschreibung der dummen Kennzahl"));
        return measureList;
    }

    private int getBinomial(int n, double p) {
        double log_q = Math.log(1.0 - p);
        int x = 0;
        double sum = 0;
        for (; ; ) {
            sum += Math.log(Math.random()) / (n - x);
            if (sum < log_q) {
                return x;
            }
            x++;
        }
    }
}
