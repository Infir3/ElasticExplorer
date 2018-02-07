package com.sb.elastic;

import com.sb.elastic.model.MovieRating;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
public class MovieLensController {

    private static final Logger LOG = LoggerFactory.getLogger(MovieLensController.class);

    @Autowired
    private ResourceLoader resourceLoader;

    @RequestMapping("/createMl100kIndex")
    @ResponseBody
    public void createMl100kIndex() throws IOException {
        RestHighLevelClient client = this.getRemoteClient();

        BufferedReader bufferedReader;

        // get movieIds and titles
        Resource moviesResource = this.resourceLoader.getResource("classpath:u.item");
        Map<Integer, String> movieMap = new HashMap<>();
        bufferedReader = new BufferedReader(new InputStreamReader(moviesResource.getInputStream()));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] fields = line.split("\\|");
            int movieId = Integer.parseInt(fields[0]);
            String title = fields[1];
            movieMap.put(movieId, title);
        }
        bufferedReader.close();

        // get user ratings
        Resource ratingsResource = this.resourceLoader.getResource("classpath:u.data");
        bufferedReader = new BufferedReader(new InputStreamReader(ratingsResource.getInputStream()));

        List<MovieRating> movieRatingList = new ArrayList<>();

        Map<Integer, List<String>> userMovies = new HashMap<>();

        while ((line = bufferedReader.readLine()) != null) {
            String[] fields = line.split("\\t");
            int userId = Integer.parseInt(fields[0]);
            int movieId = Integer.parseInt(fields[1]);
            int rating = Integer.parseInt(fields[2]);
            String title = movieMap.get(movieId);
            MovieRating movieRating = new MovieRating();
            movieRating.setUserId(userId);
            movieRating.setMovieId(movieId);
            movieRating.setRating(rating);
            movieRating.setTitle(title);
            movieRatingList.add(movieRating);
            if (userMovies.get(userId) != null) {
                List<String> movies = userMovies.get(userId);
                movies.add(title);
                userMovies.put(userId, movies);
            } else {
                List<String> movies = new ArrayList<>();
                movies.add(title);
                userMovies.put(userId, movies);
            }
        }
        bufferedReader.close();

        // put data into elasticsearch
        BulkRequest request = new BulkRequest();
        for (Map.Entry<Integer,List<String>> pair : userMovies.entrySet()) {
            Map<String, Object> jsonMap = new HashMap<>();

            jsonMap.put("userId", pair.getKey());
            jsonMap.put("movies", pair.getValue());

            request.add(new IndexRequest("ml-100k", "user", pair.getKey().toString())
                    .source(jsonMap));
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
