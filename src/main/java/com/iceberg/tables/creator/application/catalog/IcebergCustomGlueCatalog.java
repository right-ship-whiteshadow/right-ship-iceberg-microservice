package com.iceberg.tables.creator.application.catalog;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = IcebergCustomCatalogDeserializer.class)
public class IcebergCustomGlueCatalog extends GlueCatalog {

	private String name;
    private String metastoreUri;
    private Map<String, String> properties = new HashMap<String, String>();
    private Configuration configuration;
    
    public IcebergCustomGlueCatalog(String name, Map<String, String> properties, Configuration configuration) {
    	super();
    	this.name = name;
    	this.properties = properties;
    	this.setConf(configuration);
    	this.initialize(name, properties);
    }

    public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMetastoreUri() {
		return metastoreUri;
	}

	public void setMetastoreUri(String metastoreUri) {
		this.metastoreUri = metastoreUri;
	}

	public Map<String, String> getProperties() {
		return super.properties();
	}


	public Configuration getCatalogConfiguration() {
		return this.configuration;
	}

	public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Name: " + name);
        sb.append("\n");
        sb.append("Metastore Uri: " + metastoreUri);
        sb.append("\n");
        sb.append("Properties: " + properties);
        sb.append("\n");
        return sb.toString();
    }
    
    
}
