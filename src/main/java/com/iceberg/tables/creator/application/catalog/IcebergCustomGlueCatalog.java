package com.iceberg.tables.creator.application.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import software.amazon.awssdk.awscore.client.config.AwsAdvancedClientOption;
import software.amazon.awssdk.services.sts.model.Tag;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
@JsonDeserialize(using = IcebergCustomCatalogDeserializer.class)
public class IcebergCustomGlueCatalog extends GlueCatalog {

	private String name;
    private String metastoreUri;
    private Map<String, String> properties;
    private Configuration configuration;
    
    public IcebergCustomGlueCatalog() {
    	System.setProperty("aws.region", "us-west-2");
    	this.name = "GLUE_".concat(UUID.randomUUID().toString());
    	this.properties = new HashMap<>();
		this.configuration = new Configuration();
    	this.setConf(configuration);
    	this.initialize(name, properties);
    }

	public IcebergCustomGlueCatalog(String name, Map<String, String> properties, Configuration configuration) {
		super();
		System.setProperty("aws.region", "us-west-2");
		this.name = "GLUE_".concat(UUID.randomUUID().toString());
		this.properties = properties;
		this.configuration = configuration;
		this.setConf(configuration);
		//properties.put(AwsAdvancedClientOption.ENABLE_DEFAULT_REGION_DETECTION.toString(), "false");
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
    
    
}
