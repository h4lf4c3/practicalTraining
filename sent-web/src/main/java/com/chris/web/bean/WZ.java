package com.chris.web.bean;

public class WZ {
    private String id;
    private String sent_id;
    private String user_id;
    private String source;
    private String reposts_count;
    private String comments_count;
    private String attitudes_count;
    private String text;


    public WZ() {
    }

    public WZ(String id, String sent_id, String user_id, String source, String reposts_count, String comments_count, String attitudes_count, String text) {
        this.id = id;
        this.sent_id = sent_id;
        this.user_id = user_id;
        this.source = source;
        this.reposts_count = reposts_count;
        this.comments_count = comments_count;
        this.attitudes_count = attitudes_count;
        this.text = text;
    }

    @Override
    public String toString() {
        return "WZ{" +
                "id='" + id + '\'' +
                ", sent_id='" + sent_id + '\'' +
                ", user_id='" + user_id + '\'' +
                ", source='" + source + '\'' +
                ", reposts_count='" + reposts_count + '\'' +
                ", comments_count='" + comments_count + '\'' +
                ", attitudes_count='" + attitudes_count + '\'' +
                ", text='" + text + '\'' +
                '}';
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSent_id() {
        return sent_id;
    }

    public void setSent_id(String sent_id) {
        this.sent_id = sent_id;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getReposts_count() {
        return reposts_count;
    }

    public void setReposts_count(String reposts_count) {
        this.reposts_count = reposts_count;
    }

    public String getComments_count() {
        return comments_count;
    }

    public void setComments_count(String comments_count) {
        this.comments_count = comments_count;
    }

    public String getAttitudes_count() {
        return attitudes_count;
    }

    public void setAttitudes_count(String attitudes_count) {
        this.attitudes_count = attitudes_count;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
