package com.mylnikov.model;

import java.io.Serializable;

public class Message implements Serializable {


    private String location;
    private String userName;
    private String text;

    public Message() {
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
