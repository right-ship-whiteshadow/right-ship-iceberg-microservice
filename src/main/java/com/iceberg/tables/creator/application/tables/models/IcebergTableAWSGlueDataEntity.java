package com.iceberg.tables.creator.application.tables.models;

import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;

import com.google.gson.JsonObject;
import com.iceberg.tables.creator.application.service.IcebergTablesAWSGLueDataService;

import lombok.Data;


//@Data
public class IcebergTableAWSGlueDataEntity implements Serializable {

    private TableIdentifier tableIdentifier;
    private JsonObject dataJsonObject;
    private Table icebergTable;
    private GlueCatalog glueCatalog;
    private Configuration configuration;
    private Long snapshotId;
    private String scanFilter;
    private TableScan scan;

    public IcebergTableAWSGlueDataEntity() {
    	/*if (glueCatalog == null) {
    		glueCatalog = new GlueCatalog();
		}
		Configuration conf = new Configuration();
		conf.set("fs.s3a.access.key", awsCredentials.getAwsClientAccessKey());
		conf.set("fs.s3a.secret.key", awsCredentials.getAwsClientSecretKey());

		String endpoint = awsCredentials.getAwsEndPoint();
		if (endpoint != null) {
			conf.set("fs.s3a.endpoint", endpoint);
			conf.set("fs.s3a.path.style.access", "true");
		}

		if (conf.get(IO_MANIFEST_CACHE_ENABLED) == null) {
			conf.set(IO_MANIFEST_CACHE_ENABLED, IcebergTablesAWSGLueDataService.IO_MANIFEST_CACHE_ENABLED_DEFAULT);
		}
		if (conf.get(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS) == null) {
			conf.set(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
					IcebergTablesAWSGLueDataService.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT);
		}
		Map<String, String> properties = new HashMap<>();
		properties.put("list-all-tables", "true");
		this.setConfiguration(conf);
		catalog.setConf(conf);
		catalog.initialize(endpoint, properties);*/
    }
    
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
