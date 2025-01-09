package com.iceberg.tables.creator.application.tables.models;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;

import com.google.gson.JsonObject;

import lombok.Data;


//@Data
public class IcebergTableAWSGlueDataEntity implements Serializable {

    private TableIdentifier tableIdentifier;
    private JsonObject dataJsonObject;
    private Table icebergTable;
    private GlueCatalog glueCatalog;
    private Configuration configuration;
    
    private final static String iceberg_version = "2";

	public TableIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	public void setTableIdentifier(TableIdentifier tableIdentifier) {
		this.tableIdentifier = tableIdentifier;
	}

	public JsonObject getDataJsonObject() {
		return dataJsonObject;
	}

	public void setDataJsonObject(JsonObject dataJsonObject) {
		this.dataJsonObject = dataJsonObject;
	}

	public Table getIcebergTable() {
		return icebergTable;
	}

	public void setIcebergTable(Table icebergTable) {
		this.icebergTable = icebergTable;
	}

	public GlueCatalog getGlueCatalog() {
		return glueCatalog;
	}

	public void setGlueCatalog(GlueCatalog glueCatalog) {
		this.glueCatalog = glueCatalog;
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

    

}
