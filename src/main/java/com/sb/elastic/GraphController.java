package com.sb.elastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sb.elastic.model.Report;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class GraphController {

    private static final Logger LOG = LoggerFactory.getLogger(GraphController.class);

    private static final String INDEX_REPORTING_DATA = "reporting_data";

    @RequestMapping("/createTemplateIndex")
    @ResponseBody
    public void createTemplateIndex() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        try {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(INDEX_REPORTING_DATA);
            DeleteIndexResponse deleteIndexResponse = client.indices().delete(deleteIndexRequest);
        } catch (ElasticsearchStatusException e) {
            LOG.error(e.getMessage(), e);
        }

        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_REPORTING_DATA);
        CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest);

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

        // instance a json mapper
        ObjectMapper mapper = new ObjectMapper(); // create once, reuse

        BulkRequest request = new BulkRequest();
        for (Report report : reportList) {
            String stringifiedJson = mapper.writeValueAsString(report);
            request.add(new IndexRequest(INDEX_REPORTING_DATA, "report")
                .source(stringifiedJson, XContentType.JSON));
        }

        BulkResponse bulkResponse = client.bulk(request);
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            if (bulkItemResponse.isFailed()) {
                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                LOG.error(failure.getMessage());
            }
        }
        client.close();
    }

    private RestHighLevelClient getRemoteClient() {
        return new RestHighLevelClient(
            RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
    }
}
