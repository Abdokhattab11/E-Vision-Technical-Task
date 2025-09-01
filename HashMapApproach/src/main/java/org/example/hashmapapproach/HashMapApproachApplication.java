package org.example.hashmapapproach;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.hashmapapproach.service.SimilarityService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class HashMapApproachApplication implements CommandLineRunner {

    private final SimilarityService similarityService;

    public static void main(String[] args) {
        SpringApplication.run(HashMapApproachApplication.class, args);
    }


    @Override
    public void run(String... args) throws IOException {
        similarityService.compareFilesSimilarityService();
    }
}
