package com.example.timestream.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;

import java.time.Duration;

/**
 * @author sa
 * @date 1.04.2021
 * @time 17:32
 */
@Configuration
public class TimestreamConfig
{
    //https://docs.aws.amazon.com/timestream/latest/developerguide/accessing.html#SettingUp.Q.GetCredentials
    @Value("${aws.access-key}")
    private String AWS_ACCESS_ID;

    @Value("${aws.secret-key}")
    private String AWS_SECRET;

    @Bean
    public TimestreamQueryClient queryClient() {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(AWS_ACCESS_ID, AWS_SECRET);

        return TimestreamQueryClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
    }

    /**
     * Recommended Timestream write client SDK configuration:
     *  - Set SDK retry count to 10.
     *  - Use SDK DEFAULT_BACKOFF_STRATEGY
     *  - Set RequestTimeout to 20 seconds .
     *  - Set max connections to 5000 or higher.
     */
    @Bean
    public TimestreamWriteClient writeClient() {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(AWS_ACCESS_ID, AWS_SECRET);
        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();
        httpClientBuilder.maxConnections(5000);

        RetryPolicy.Builder retryPolicy = RetryPolicy.builder();
        retryPolicy.numRetries(10);

        ClientOverrideConfiguration.Builder overrideConfig = ClientOverrideConfiguration.builder();
        overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20));
        overrideConfig.retryPolicy(retryPolicy.build());

        return TimestreamWriteClient.builder()
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig.build())
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
    }
}
