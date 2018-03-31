package com.bigdaf.sparkmask.vo;

import java.io.Serializable;

public class MaskVo implements Serializable{
    private String dbName;
    private String tables;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }
}
