package model;

public class MessageInner extends Message {
    private String messageId;

    private int partition;

    private String host;

    private String broker;

    private long physicalOffset;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }


    public long getPhysicalOffset() {
        return physicalOffset;
    }

    public void setPhysicalOffset(long physicalOffset) {
        this.physicalOffset = physicalOffset;
    }
}
