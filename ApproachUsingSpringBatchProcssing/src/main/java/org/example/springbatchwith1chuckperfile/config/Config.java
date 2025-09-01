
package org.example.springbatchwith1chuckperfile.config;

import lombok.extern.slf4j.Slf4j;
import org.example.springbatchwith1chuckperfile.dto.FinalResult;
import org.example.springbatchwith1chuckperfile.dto.SimilarityResult;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.example.springbatchwith1chuckperfile.dto.SimilarityResult.performComparison;

@Configuration
@Slf4j
public class Config {

    /**
     * To hold final result of all similarity comparisons
     * */
    private final FinalResult finalResult = new FinalResult();

    /**
     * Resource for Directory that contains Pool of files to be compared with File A
     * */
    @Value("${input.pool}")
    private Resource[] dirPool;


    /**
     * Main input file A
     * */
    @Value("${input.fileA}")
    private String inputFileA;

    /**
     * Thread-safe Map to hold word frequency of File A
     * To be used in parallel processing, when comparing each file in the pool with File A
     * */
    private final Map<String, Integer> fileAFrequency = new ConcurrentHashMap<>();

    /**
     * Main Job Definition
     * 1. Step to process File A and populate word frequency map
     * 2. Master Step to partition and process files in the directory pool in parallel
     * 3. Listener to log final results after job completion
     * */
    @Bean
    public Job wordFrequencyJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws IOException {
        return new JobBuilder("wordFrequencyJob", jobRepository)
                .start(fileAStep(jobRepository, transactionManager))
                .next(masterStep(jobRepository, transactionManager))
                .listener(jobCompletionListener())
                .build();
    }
    /**
     * Step to process File A and populate word frequency map
     * @return Step that will read, process and write word frequencies of File A
     * **/
    @Bean
    public Step fileAStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("fileAStep", jobRepository)
                // Chunk Size is adjustable based on input file format
                // Return of each Chunk is a Map of word frequencies for each line
                .<String, Map<String, Integer>>chunk(20, transactionManager)
                .reader(fileAReader())
                .processor(wordFrequencyProcessor())
                .writer(fileAWriter())
                .build();
    }
    /**
     * Reader of file A
     * @return FlatFileItemReader that reads each line as a String
     * */
    @Bean
    public FlatFileItemReader<String> fileAReader() {
        return new FlatFileItemReaderBuilder<String>()
                .name("fileAReader")
                .resource(new ClassPathResource(inputFileA))
                // Each line is read as a String
                .lineMapper(new PassThroughLineMapper())
                .build();
    }
    /**
     * Logic of processing each line to compute word frequency
     * @return ItemProcessor that converts each line into a Map of word frequencies
     * */
    @Bean
    public ItemProcessor<String, Map<String, Integer>> wordFrequencyProcessor() {
        return line -> {
            Map<String, Integer> map = new HashMap<>();
            String[] words = line.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.matches("[a-zA-Z]+")) {
                    map.put(word, map.getOrDefault(word, 0) + 1);
                }
            }
            return map;
        };
    }
    /**
     * For Each Map from processor, merge into the main fileAFrequency map
     * */
    @Bean
    public ItemWriter<Map<String, Integer>> fileAWriter() {
        return items -> {
            log.info("Writer received {} chunks.", items.size());
            for (Map<String, Integer> item : items) {
                item.forEach((key, value) -> fileAFrequency.merge(key, value, Integer::sum));
            }
        };
    }


    // ---------------------------------- Directory Pool Configuration ----------------------------------

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("thread-exec-");
    }
    /**
     * Here We Use The Idea of Having a main step which Read the Directory Pool
     * Then For Each File in the Pool, it will create a partition for it
     * Then Each Partition will be processed in parallel by the Slave step
     * Each Slave Step will read 1 file at a time (chunk size = 1)
     * Each Slave Step will compare the file with File A using the pre-computed word frequency
     * */
    @Bean
    public Step masterStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws MalformedURLException {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", filePartitioner())
                .gridSize(dirPool.length)
                .step(slaveStep(jobRepository, transactionManager))
                .taskExecutor(taskExecutor())
                .build();
    }

    /**
     * Map each file in the directory pool to a partition
     * URL, and filename will be stored in the ExecutionContext for use in the Slave Step
     * */
    @Bean
    public Partitioner filePartitioner() {
        return gridSize -> {
            Map<String, ExecutionContext> partitions = new HashMap<>();
            int i = 0;
            for (Resource resource : dirPool) {
                ExecutionContext context = new ExecutionContext();
                try {
                    context.putString("resource", resource.getURL().toString());
                    context.putString("fileName", resource.getFilename());
                } catch (IOException e) {
                    throw new RuntimeException("Could not get URL for resource: " + resource.getFilename(), e);
                }
                partitions.put("partition" + i, context);
                i++;
            }
            log.info("Created {} partitions for parallel processing.", partitions.size());
            return partitions;
        };
    }
    /**
     * Each Slave Step, will have 1 Resource, and expected output is the similarity Score with File A
     * Chunk Size is 1, as we want to process 1 file at a time
     * */
    @Bean
    public Step slaveStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) throws MalformedURLException {
        return new StepBuilder("workerStep", jobRepository)
                .<Resource, SimilarityResult>chunk(1, transactionManager)
                .reader(partitionedFileReader(null))
                .processor(fileComparisonProcessor())
                .writer(comparisonResultWriter())
                .build();
    }

    /**
     * Reader for each partition, and get the Resource from the ExecutionContext
     * */
    @Bean
    @StepScope
    public ListItemReader<Resource> partitionedFileReader(
            @Value("#{stepExecutionContext['resource']}") String resourceUrl) throws MalformedURLException {
        if (resourceUrl == null) {
            return new ListItemReader<>(Collections.emptyList());
        }
        Resource resource = new UrlResource(resourceUrl);
        return new ListItemReader<>(List.of(resource));
    }

    /**
     * For Each File in the Pool, Read it using a FlatFileItemReader
     * Process each line to compute word frequency using the same processor as File A
     * Then Compare the computed word frequency with the pre-computed word frequency of File A
     * Return a ComparisonResult object containing the filename, total word count and intersection count
     * */
    @Bean
    @StepScope
    public ItemProcessor<Resource, SimilarityResult> fileComparisonProcessor() {
        return resource -> {
            log.info("Processing file: {} on thread: {} ", resource.getFilename(), Thread.currentThread().getName());

            FlatFileItemReader<String> reader = new FlatFileItemReaderBuilder<String>()
                    .name("temp-reader-" + resource.getFilename())
                    .resource(resource)
                    .lineMapper(new PassThroughLineMapper())
                    .build();

            // Initialize the reader
            reader.afterPropertiesSet();

            ItemProcessor<String, Map<String, Integer>> lineProcessor = wordFrequencyProcessor();
            Map<String, Integer> currentFileFrequency = new HashMap<>();

            reader.open(new ExecutionContext());
            String line;
            while ((line = reader.read()) != null) {
                Map<String, Integer> lineMap = lineProcessor.process(line);
                lineMap.forEach((key, value) -> currentFileFrequency.merge(key, value, Integer::sum));
            }
            reader.close();

            return performComparison(resource.getFilename(), fileAFrequency, currentFileFrequency);
        };
    }
    /**
     * For Now Writer Only Logs the Comparison Result
     * We can extend it to write to a DB or a file as needed
     * */
    @Bean
    public ItemWriter<SimilarityResult> comparisonResultWriter() {
        return chunk -> {
            for (SimilarityResult result : chunk) {
                finalResult.addSimilarityResult(result);
            }
        };
    }

    /**
     * Print Result in sorted order after Job Completion
     * */
    @Bean
    public JobExecutionListener jobCompletionListener() {
        return new JobExecutionListener() {
            @Override
            public void afterJob(JobExecution jobExecution) {
                finalResult.sortByScoreDescending();
                log.info("Job Completed. Similarity Results:");
                for (SimilarityResult result : finalResult.getSimilarities()) {
                    log.info("File: {}, Total Words: {}, Intersection: {}, Similarity Score: {}%",
                            result.getFilename(), result.getTotalCount(), result.getIntersectionCount(), String.format("%.2f", result.getSimilarityScore()));
                }
            }
        };
    }

}
