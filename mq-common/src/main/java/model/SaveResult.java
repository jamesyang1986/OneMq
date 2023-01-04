package model;

import java.io.Serializable;

public class SaveResult implements Serializable {
    private boolean saveOk;

    private String host;

    private int partition;
}
