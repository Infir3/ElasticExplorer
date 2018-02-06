package com.sb.elastic.model;

public class MovieRating {
    private int movieId;
    private String title;
    private int userId;
    private int rating;

    public MovieRating() {

    }

    public MovieRating(int movieId, String title, int userId, int rating) {
        this.movieId = movieId;
        this.title = title;
        this.userId = userId;
        this.rating = rating;
    }

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }
}
