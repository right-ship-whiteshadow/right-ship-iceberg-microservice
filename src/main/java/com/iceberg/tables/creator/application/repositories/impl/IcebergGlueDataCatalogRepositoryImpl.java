package com.iceberg.tables.creator.application.repositories.impl;

import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.glue.GlueCatalog;

import com.iceberg.tables.creator.application.repositories.IcebergGlueDataCatalogRepository;
import com.iceberg.tables.creator.application.security.AWSCredentials;
import com.iceberg.tables.creator.application.service.IcebergTablesAWSGLueDataService;
import com.iceberg.tables.creator.application.tables.datas.IcebergTableAWSGlueDataEntities;
import com.iceberg.tables.creator.application.tables.models.IcebergTableAWSGlueDataEntity;

import jakarta.annotation.PostConstruct;
import software.amazon.awssdk.services.glue.GlueClient;

public class IcebergGlueDataCatalogRepositoryImpl implements IcebergGlueDataCatalogRepository {

	private GlueClient glueClient;

	private AWSCredentials awsCredentials;

	private IcebergTableAWSGlueDataEntities icebergTableAWSGlueDataEntities;

	public IcebergGlueDataCatalogRepositoryImpl(GlueClient glueClient, AWSCredentials awsCredentials) {
		this.glueClient = glueClient;
		this.awsCredentials = awsCredentials;
	}

	@PostConstruct
	public void setUpIcebergRepository() {
		if (icebergTableAWSGlueDataEntities == null) {
			icebergTableAWSGlueDataEntities = new IcebergTableAWSGlueDataEntities();
		}
		List<IcebergTableAWSGlueDataEntity> iceAwsGlueDataEntities = icebergTableAWSGlueDataEntities
				.getIcebergUniversalTableEntities();
		if (awsCredentials != null && CollectionUtils.isNotEmpty(iceAwsGlueDataEntities)) {
			iceAwsGlueDataEntities.forEach(icebergTableEntity -> {
				GlueCatalog catalog = icebergTableEntity.getGlueCatalog();
				if (catalog == null) {
					catalog = new GlueCatalog();
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
				icebergTableEntity.setConfiguration(conf);
				catalog.setConf(conf);
				catalog.initialize(endpoint, properties)
			});

			

		}

	}

}
