package com.iceberg.tables.creator.application;

import com.iceberg.tables.creator.application.constants.IcebergConstants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mail.MailSenderAutoConfiguration;

import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class IcebergTablesCreatorMicroserviceApplication {

    /*@Value("cloud.aws.credentials.access-key")
    private static String awsClientSecretKey;

    @Value("cloud.aws.credentials.secret-key")
    private static String awsClientAccessKey;

    @Value("cloud.aws.region.static")
    private static String awsRegion;

    @Value("cloud.aws.session.token")
    private static String awsSessionToken;

    @Value("cloud.aws.endpoint.url")
    private static String awsEndPoint;*/

    //@Value("cloud.aws.credentials.access-key")
    private static String awsClientSecretKey = "Test1";

    //@Value("cloud.aws.credentials.secret-key")
    private static String awsClientAccessKey = "Test2";

    //@Value("cloud.aws.region.static")
    private static String awsRegion = "eu-central-1";

    //@Value("cloud.aws.session.token")
    private static String awsSessionToken = "Token";

    //@Value("cloud.aws.endpoint.url")
    private static String awsEndPoint = "URL";

    public static void initializeAwsCredentialsInSystemVariables() {
		/*System.getenv().put(IcebergConstants.awsAccessKeyId, awsClientAccessKey);
		System.getenv().put(IcebergConstants.awsSecretAccessKey, awsClientSecretKey);
		System.getenv().put(IcebergConstants.awsSessionToken, awsSessionToken);
		System.getenv().put(IcebergConstants.awsAccessKeyIdSystemVariables, awsClientAccessKey);
		System.getenv().put(IcebergConstants.awsSecretAccessKeySystemVariables, awsClientSecretKey);
		System.getenv().put(IcebergConstants.awssessionTokenSystemVariables, awsSessionToken);*/
        Properties systemProperties = System.getProperties();
        if(systemProperties == null) {
            systemProperties = new Properties();
            System.setProperties(systemProperties);
        }
        System.getProperties().put(IcebergConstants.awsRegion, "eu-central-1");
        System.getProperties().put("aws.region", "eu-central-1");
        System.getProperties().put(IcebergConstants.awsDefaultRegion, "eu-central-1");
        System.getProperties().put(IcebergConstants.awsAccessKeyId, awsClientAccessKey);
        System.getProperties().put(IcebergConstants.awsAccessKeyId, awsClientAccessKey);
        System.getProperties().put(IcebergConstants.awsSecretAccessKey, awsClientSecretKey);
        System.getProperties().put(IcebergConstants.awsSessionToken, awsSessionToken);
        System.getProperties().put(IcebergConstants.awsAccessKeyIdSystemVariables, awsClientAccessKey);
        System.getProperties().put(IcebergConstants.awsSecretAccessKeySystemVariables, awsClientSecretKey);
        System.getProperties().put(IcebergConstants.awssessionTokenSystemVariables, awsSessionToken);
    }

    public static void main(String[] args) {
        initializeAwsCredentialsInSystemVariables();
        SpringApplication.run(IcebergTablesCreatorMicroserviceApplication.class, args);
    }

}
