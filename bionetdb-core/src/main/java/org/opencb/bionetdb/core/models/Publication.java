package org.opencb.bionetdb.core.models;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dapregi on 14/09/15.
 */
public class Publication {

    private String source;
    private String id;
    private String title;
    private String journal;
    private int year;
    private List<String> authors;

    public Publication() {
        init();
    }

    public Publication(String source, String id) {
        init();
        if (source != null) {
            this.source = source.toLowerCase();
        }
        if (id != null) {
            this.id = id;
        }
    }

    private void init() {
        this.source = "";
        this.id = "";
        this.title = "";
        this.journal = "";
        this.year = -1;
        this.authors = new ArrayList<>();
    }

    public boolean isEqual(Publication that) {
        return this.getId().equals(that.getId()) && this.getSource().equals(that.getSource());
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        if (source != null) {
            this.source = source.toLowerCase();
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        if (id != null) {
            this.id = id;
        }
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getJournal() {
        return journal;
    }

    public void setJournal(String journal) {
        this.journal = journal;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public void setAuthors(List<String> authors) {
        this.authors = authors;
    }

    public void setAuthor(String author) {
        if (!this.getAuthors().contains(author)) {
            this.getAuthors().add(author);
        }
    }
}
