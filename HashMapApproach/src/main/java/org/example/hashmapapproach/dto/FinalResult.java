package org.example.hashmapapproach.dto;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
public class FinalResult {
    private final List<SimilarityResult> similarities = Collections.synchronizedList(new ArrayList<>());

    public void addSimilarityResult(SimilarityResult result) {
        similarities.add(result);
    }

    public List<SimilarityResult> getSimilarities() {
        return similarities;
    }

    public void sortByScoreDescending() {
        similarities.sort((a, b) -> Double.compare(b.getSimilarityScore(), a.getSimilarityScore()));
    }

}
