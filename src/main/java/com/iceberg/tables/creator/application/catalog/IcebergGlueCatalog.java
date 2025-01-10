package com.iceberg.tables.creator.application.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
@JsonDeserialize(using = IcebergCustomCatalogDeserializer.class)
public class IcebergGlueCatalog {

/*	private String catalogName = "Glue_".concat((new Random()).toString());
    private String metastoreUri;
    private Map<String, String> catalogProperties = new HashMap<>();
    private Configuration hadoopConfiguration = new Configuration();
	private AwsProperties awsProperties = new AwsProperties();
	private S3FileIOProperties s3Properties;
    private GlueCatalog glueCatalog;

    public IcebergGlueCatalog() {
    	System.setProperty("aws.region", "us-west-2");
    	this.catalogName = "GLUE_".concat(UUID.randomUUID().toString());
    	this.catalogProperties = new HashMap<>();
		this.configuration = new Configuration();
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
		return properties;
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
    */
    
}
