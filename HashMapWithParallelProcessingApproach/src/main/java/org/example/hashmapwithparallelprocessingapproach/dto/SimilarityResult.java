package org.example.hashmapwithparallelprocessingapproach.dto;

import java.util.Map;

public class SimilarityResult {
    private final String filename;
    private final long totalCount;
    private final long intersectionCount;
    private final double similarityScore;


    public SimilarityResult(String filename, long totalCount, long intersectionCount, double similarityScore) {
        this.filename = filename;
        this.totalCount = totalCount;
        this.intersectionCount = intersectionCount;
        this.similarityScore = similarityScore;
    }

    public String getFilename() {
        return filename;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public long getIntersectionCount() {
        return intersectionCount;
    }
    public double getSimilarityScore() {
        return similarityScore;
    }

    public static SimilarityResult performComparison(String filename, Map<String, Integer> mapA, Map<String, Integer> mapB) {
        long intersectionCount = 0;
        long unionCount = 0;
        for(String key : mapA.keySet()){
            intersectionCount += Math.min(mapA.getOrDefault(key, 0), mapB.getOrDefault(key,0));
            unionCount += Math.max(mapA.getOrDefault(key, 0), mapB.getOrDefault(key,0));
        }
        double score = unionCount == 0 ? 0.0 : ((double) intersectionCount / unionCount) * 100.0;
        return new SimilarityResult(filename, unionCount, intersectionCount, score);
    }
}