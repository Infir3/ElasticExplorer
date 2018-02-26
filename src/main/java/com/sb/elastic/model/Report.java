package com.sb.elastic.model;

import java.util.ArrayList;
import java.util.List;

public class Report {
    private String title;
    private String description;
    private List<Integer> measures = new ArrayList<>();

    public Report(String title, String description) {
        this.title = title;
        this.description = description;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Integer> getMeasures() {
        return measures;
    }

    public void setMeasures(List<Integer> measures) {
        this.measures = measures;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
