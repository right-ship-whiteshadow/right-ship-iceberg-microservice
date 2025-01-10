package com.iceberg.tables.creator.application.config;

import com.iceberg.tables.creator.application.repositories.impl.IcebergGlueDataCatalogRepositoryImpl;
import com.iceberg.tables.creator.application.security.AWSCredentials;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import com.iceberg.tables.creator.application.repositories.IcebergGlueDataCatalogRepository;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class IcebergConfig {

	static {
		System.getProperties().put("aws.region", "eu-central-1");
	}

	@Bean
	public AWSCredentials getAWSCredentials() {
		return new AWSCredentials();
	}

	@Bean
	public AwsSessionCredentials getAwsSessionCredentials() {
		AWSCredentials awsCredentials = getAWSCredentials();
		return AwsSessionCredentials.create(awsCredentials.getAwsClientAccessKey(),
				awsCredentials.getAwsClientSecretKey(), awsCredentials.getAwsSessionToken());
	}
	
	@Bean
	public AwsCredentialsProvider getAwsCredentialsProvider() {
		return StaticCredentialsProvider.create(getAwsSessionCredentials());
	}

	@Bean
	public GlueClient getGlueClient() {
		return GlueClient.builder().region(Region.EU_CENTRAL_1)
				.credentialsProvider(getAwsCredentialsProvider()).build();
	}
	
	@Bean
	public GlueCatalog getGlueCatalog() {
		return new GlueCatalog();
	}
	
	@Bean 
	@Scope("prototype")
	public IcebergGlueDataCatalogRepository getIcebergGlueDataCatalogRepository() {
		return new IcebergGlueDataCatalogRepositoryImpl(getGlueCatalog(), getGlueClient(),
				getAWSCredentials());
	}

	@Bean
	public S3Client getS3Client() {
		return S3Client.builder().region(Region.EU_CENTRAL_1)
				.credentialsProvider(getAwsCredentialsProvider())
				.build();
	}
}
