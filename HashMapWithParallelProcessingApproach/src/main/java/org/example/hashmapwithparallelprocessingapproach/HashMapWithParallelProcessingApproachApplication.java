package org.example.hashmapwithparallelprocessingapproach;

import lombok.RequiredArgsConstructor;
import org.example.hashmapwithparallelprocessingapproach.service.SimilarityService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
@RequiredArgsConstructor
public class HashMapWithParallelProcessingApproachApplication implements CommandLineRunner {


    private final SimilarityService similarityService;

    public static void main(String[] args) {
        SpringApplication.run(HashMapWithParallelProcessingApproachApplication.class, args);
    }

    @Override
    public void run(String... args) throws IOException {
        similarityService.compareFilesSimilarityService();
    }
}
