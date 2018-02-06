package com.sb.elastic;

import com.sb.elastic.model.MovieRating;
import org.apache.http.HttpHost;
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

        // get movieIds and titles
        Resource moviesResource = this.resourceLoader.getResource("classpath:u.item");
        Map<Integer, String> movies = new HashMap<>();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(moviesResource.getInputStream()));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] fields = line.split("\\|");
            int movieId = Integer.parseInt(fields[0]);
            String title = fields[1];
            movies.put(movieId, title);
        }
        bufferedReader.close();

        // get user ratings
        Resource ratingsResource = this.resourceLoader.getResource("classpath:u.data");
        bufferedReader = new BufferedReader(new InputStreamReader(ratingsResource.getInputStream()));

        List<MovieRating> movieRatingList = new ArrayList<>();

        while ((line = bufferedReader.readLine()) != null) {
            String[] fields = line.split("\\t");
            int userId = Integer.parseInt(fields[0]);
            int movieId = Integer.parseInt(fields[1]);
            int rating = Integer.parseInt(fields[2]);
            MovieRating movieRating = new MovieRating();
            movieRating.setUserId(userId);
            movieRating.setMovieId(movieId);
            movieRating.setRating(rating);
            movieRating.setTitle(movies.get(movieId)); // get title
            movieRatingList.add(movieRating);
        }
        bufferedReader.close();

        client.close();
    }

    private RestHighLevelClient getRemoteClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
    }
}
