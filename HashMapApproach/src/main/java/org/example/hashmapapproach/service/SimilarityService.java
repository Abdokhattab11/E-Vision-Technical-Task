package org.example.hashmapapproach.service;

import lombok.extern.slf4j.Slf4j;
import org.example.hashmapapproach.dto.FinalResult;
import org.example.hashmapapproach.dto.SimilarityResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class SimilarityService {

    /**
     * File A
     * */
    @Value("${input.fileA}")
    private Resource fileAPath;

    /**
     * Pool Directory
     * */
    @Value("${input.pool}")
    private Resource[] poolDir;

    private final Map<String, Integer> fileAFreq = new HashMap<>();
    private final FinalResult finalResult = new FinalResult();


    /**
     * Main Service to compare File A with all files in Pool Directory
     * */
    public void compareFilesSimilarityService() throws IOException {

        long startTime = System.nanoTime();

        loadFileAFreq();
        for(Resource r : poolDir){
            calculateSimilarity(r);
        }
        finalResult.sortByScoreDescending();

        long endTime = System.nanoTime();
        long elapsedTime = (endTime - startTime) / 1_000_000;

        log.info("Similarity results After Sorting : {}", finalResult.getSimilarities());
        for(SimilarityResult result : finalResult.getSimilarities()){
            log.info("-------- File: {}, Union Count: {}, Intersection Count: {}, Similarity Score: {} --------",
                    result.getFilename(), result.getTotalCount(), result.getIntersectionCount(), result.getSimilarityScore());
        }

        log.info("Total time to process all files: {} ms", elapsedTime);

    }
    /**
     * Load File A words in a HashMap
     * */
    public void loadFileAFreq() throws IOException {
        log.info("Start to compare files similarity service");
        long startTime = System.nanoTime();
        InputStream inputStream = fileAPath.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while((line = reader.readLine()) != null) {
            String[] words = line.split("\\W+");
            for(String word : words){
                if(word.matches("[a-zA-Z]+")){
                    fileAFreq.put(word, fileAFreq.getOrDefault(word, 0) + 1);
                }
            }
        }
        reader.close();
        long endTime = System.nanoTime();
        long elapsedTime = (endTime - startTime) / 1_000_000;
        log.info("File A loaded in {} ms", elapsedTime);
    }
    /**
     * For Each File in resource Pool
     * Calculate Similarity with File A
     * And Save it in FinalResult, to be sorted and displayed later
     * */
    public void calculateSimilarity(Resource file){
        log.info("Start to calculate similarity of file: {}", file.getFilename());
        long startTime = System.nanoTime();
        Map<String, Integer> fileFreq = new HashMap<>();
        try (BufferedReader  reader = new BufferedReader(new InputStreamReader(file.getInputStream()))){
            String line;
            while((line = reader.readLine()) != null) {
                String[] words = line.split("\\W+");
                for(String word : words){
                    if(word.matches("[a-zA-Z]+")){
                        fileFreq.put(word, fileFreq.getOrDefault(word, 0) + 1);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
        long intersectionCount = 0;
        long unionCount = 0;
        for(String word : fileFreq.keySet()){
            intersectionCount += Math.min(fileFreq.getOrDefault(word, 0), fileAFreq.getOrDefault(word,0));
            unionCount += Math.max(fileFreq.getOrDefault(word, 0), fileAFreq.getOrDefault(word,0));
        }
        double score = unionCount == 0 ? 0.0 : ((double) intersectionCount / unionCount) * 100.0;

        long endTime = System.nanoTime();
        long elapsedTime = (endTime - startTime) / 1_000_000;
        log.info("File: {} processed in {} ms", file.getFilename(), elapsedTime);

        SimilarityResult result = new SimilarityResult(file.getFilename(), unionCount, intersectionCount, score);
        finalResult.addSimilarityResult(result);
    }
}
