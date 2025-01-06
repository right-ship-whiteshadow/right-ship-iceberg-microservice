package com.iceberg.tables.creator.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mail.MailSenderAutoConfiguration;

@SpringBootApplication
public class IcebergTablesCreatorMicroserviceApplication {

    public static void main(String[] args) {
        SpringApplication.run(IcebergTablesCreatorMicroserviceApplication.class, args);
    }

}
