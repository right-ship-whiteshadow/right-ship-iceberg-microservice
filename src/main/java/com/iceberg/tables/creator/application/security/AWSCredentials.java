package com.iceberg.tables.creator.application.security;

import org.springframework.stereotype.Component;

@Component
public class AWSCredentials {

    private String awsClientSecretKey;
    private String awsClientAccessKey;
    private String awsRegion;
    private String awsSessionToken;
    private String awsEndPoint;
    
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
    
	
    
    
}
