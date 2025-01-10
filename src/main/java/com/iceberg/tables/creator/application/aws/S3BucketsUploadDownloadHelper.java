package com.iceberg.tables.creator.application.aws;

import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.StsException;

import java.util.List;

@Component
public class S3BucketsUploadDownloadHelper {

    @Autowired
    private AwsSessionCredentials awsSessionCredentials;

    @Autowired
    private AwsCredentialsProvider credentialsProvider;

    @Autowired
    private S3Client s3Client;

    private static org.apache.logging.log4j.Logger log = LogManager
            .getLogger(S3BucketsUploadDownloadHelper.class);

    // The AWS IAM Identity Center identity (user) who executes this method does not have permission to list buckets.
    // The identity is configured in the [default] profile.
    // The IAM role represented by the 'roleArn' parameter can be assumed by identities in two different accounts
    // and the role permits the user to only list buckets.

    // The SDK's default credentials provider chain will find the single sign-on settings in the [default] profile.
    // The identity configured with the [default] profile needs permission to call AssumeRole on the STS service.

    public void assumeRole(String roleArn, String roleSessionName) {
        try {
            try (StsClient stsClient = StsClient.create()) {
                AssumeRoleRequest roleRequest = AssumeRoleRequest.builder()
                        .roleArn(roleArn)
                        .roleSessionName(roleSessionName)
                        .build();

                AssumeRoleResponse roleResponse = stsClient.assumeRole(roleRequest);
            }
            // Use the following temporary credential items for the S3 client.
            // List all buckets in the account associated with the assumed role
            // by using the temporary credentials retrieved by invoking stsClient.assumeRole().
            List<Bucket> buckets = s3Client.listBuckets().buckets();
            for (Bucket bucket : buckets) {
                System.out.println("bucket name: " + bucket.name());
            }

        } catch (StsException | S3Exception e) {
            log.error(e.getMessage());
        }
    }
}
