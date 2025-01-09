package com.iceberg.tables.creator.application.config;

import com.iceberg.tables.creator.application.repositories.impl.IcebergGlueDataCatalogRepositoryImpl;
import com.iceberg.tables.creator.application.security.AWSCredentials;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.iceberg.tables.creator.application.repositories.IcebergGlueDataCatalogRepository;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;

@Configuration
public class IcebergConfig {
	
	@Bean
	public AWSCredentials getAWSCredentials() {
		return new AWSCredentials();
	}
	
	@Bean
	public AwsCredentialsProvider getAwsCredentialsProvider() {
		
	}

	@Bean
	public GlueClient getGlueClient() {
		return GlueClient.builder().region(Region.EU_CENTRAL_1)
				.credentialsProvider(null).build();

	}
	
	@Bean
	public GlueCatalog getGlueCatalog() {
		return new GlueCatalog();

	}
	
	@Bean 
	@Scope("prototype")
	public IcebergGlueDataCatalogRepository getIcebergGlueDataCatalogRepository() {
		return new IcebergGlueDataCatalogRepositoryImpl(getGlueClient(),
				getAWSCredentials());
	}

}
