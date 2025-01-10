package com.iceberg.tables.creator.application.security;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.iceberg.tables.creator.application.constants.IcebergConstants;

import jakarta.annotation.PostConstruct;

/*
@Getter
@Setter
@NoArgsConstructor*/
@Component
public class AWSCredentials {

	@Value("cloud.aws.credentials.access-key")
    private String awsClientSecretKey = "Test1";
	
	@Value("cloud.aws.credentials.secret-key")
    private String awsClientAccessKey = "Test2";
	
	@Value("cloud.aws.region.static")
    private String awsRegion = "eu-central-1";
	
	@Value("cloud.aws.session.token")
    private String awsSessionToken = "Token";
	
	@Value("cloud.aws.endpoint.url")
    private String awsEndPoint = "URL";
	
	private Map<String, String> awsCredentialsMap;
	
	@PostConstruct
	public void initializeAwsCredentials() {
		awsCredentialsMap = initializeCredentialsPropertiesMap();
		//initializeAwsCredentialsInSystemVariables();
	}

	public String getAwsClientSecretKey() {
		return awsClientSecretKey;
	}

	public void setAwsClientSecretKey(String awsClientSecretKey) {
		this.awsClientSecretKey = awsClientSecretKey;
	}

	public String getAwsClientAccessKey() {
		return awsClientAccessKey;
	}

	public void setAwsClientAccessKey(String awsClientAccessKey) {
		this.awsClientAccessKey = awsClientAccessKey;
	}

	public String getAwsRegion() {
		return awsRegion;
	}

	public void setAwsRegion(String awsRegion) {
		this.awsRegion = awsRegion;
	}

	public String getAwsSessionToken() {
		return awsSessionToken;
	}

	public void setAwsSessionToken(String awsSessionToken) {
		this.awsSessionToken = awsSessionToken;
	}

	public String getAwsEndPoint() {
		return awsEndPoint;
	}

	public void setAwsEndPoint(String awsEndPoint) {
		this.awsEndPoint = awsEndPoint;
	}
	
	public Map<String, String> initializeCredentialsPropertiesMap() {
		awsCredentialsMap = new HashMap<>();
		awsCredentialsMap.put(IcebergConstants.awsAccessKeyId, awsClientAccessKey);
		awsCredentialsMap.put(IcebergConstants.awsSecretAccessKey, awsClientSecretKey);
		awsCredentialsMap.put(IcebergConstants.awsSessionToken, awsSessionToken);
		awsCredentialsMap.put(IcebergConstants.awsAccessKeyIdSystemVariables, awsClientAccessKey);
		awsCredentialsMap.put(IcebergConstants.awsSecretAccessKeySystemVariables, awsClientSecretKey);
		awsCredentialsMap.put(IcebergConstants.awssessionTokenSystemVariables, awsSessionToken);
		awsCredentialsMap.put(IcebergConstants.awsRegion, awsRegion);
		awsCredentialsMap.put(IcebergConstants.awsDefaultRegion, awsRegion);
		return awsCredentialsMap;
	}


}
