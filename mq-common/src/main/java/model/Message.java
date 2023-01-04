package model;

import java.util.Map;

public class Message {
    // the topic for message
    private String topic;

    private long bornTs;

    private long saveTs;

    private byte[] content;

    private Map<String, String> properties;


    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getBornTs() {
        return bornTs;
    }

    public void setBornTs(long bornTs) {
        this.bornTs = bornTs;
    }

    public long getSaveTs() {
        return saveTs;
    }

    public void setSaveTs(long saveTs) {
        this.saveTs = saveTs;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
