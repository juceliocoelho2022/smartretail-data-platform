package com.smartretail.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class SmartRetailIngestionApplication {

    public static void main(String[] args) {
        SpringApplication.run(SmartRetailIngestionApplication.class, args);
    }
}
