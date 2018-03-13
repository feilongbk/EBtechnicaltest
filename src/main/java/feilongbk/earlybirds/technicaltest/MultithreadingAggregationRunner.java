package feilongbk.earlybirds.technicaltest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultithreadingAggregationRunner extends SimpleAggregationRunner {
    // ThreadPoolExecutor threadPoolExecutor= Executors.newFixedThreadPool(100);
    public MultithreadingAggregationRunner(InputStreamGenerator inputStreamGenerator, OutputStreamGenerator outputStreamGenerator, List<String> csvInputFilePaths, String aggregateRatingFilePath, String lookUpUserFilePath, String lookUpProductFilePath) {
        super(inputStreamGenerator, outputStreamGenerator, csvInputFilePaths, aggregateRatingFilePath, lookUpUserFilePath, lookUpProductFilePath);
    this.userIds=new ConcurrentHashMap<>();
    this.productIds=new ConcurrentHashMap<>();
    this.ratingsByUserIdThenProductIds=new ConcurrentHashMap<>();
    }

    public MultithreadingAggregationRunner(InputStreamGenerator inputStreamGenerator, OutputStreamGenerator outputStreamGenerator, List<String> csvInputFilePaths, long referenceTimeStamp, String aggregateRatingFilePath, String lookUpUserFilePath, String lookUpProductFilePath) {
        super(inputStreamGenerator, outputStreamGenerator, csvInputFilePaths, referenceTimeStamp, aggregateRatingFilePath, lookUpUserFilePath, lookUpProductFilePath);

    }


    @Override
    public long findReferenceTimeStamp() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        ConcurrentHashMap<Integer, Long> maxTimeStampByInputFileIndex = new ConcurrentHashMap<>();
        for (int i = 0; i < csvInputFilePaths.size(); i++) {
            final int threadIndex = i;
            String filePath = csvInputFilePaths.get(i);

            executor.execute(() -> {
                        long referenceTimeStampByThread = 0L;
                        try {

                            BufferedReader br = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath)));
                            DataReader dataReader = new SimpleDataReader(br);
                            System.out.println("Reading timeStamp from the input file:" + filePath + " started. Current maxTimeStamp: " + referenceTimeStampByThread + ". Equivalent date: " + new Date(referenceTimeStampByThread));
                            try {
                                long rowTimeStamp;
                                while (true) {
                                    if (referenceTimeStampByThread < (rowTimeStamp = dataReader.readTimeStamp())) {
                                        referenceTimeStampByThread = rowTimeStamp;
                                    }
                                }
                            } catch (NullPointerException e) {
                                System.out.println("Reading timeStamp from the input file:" + filePath + " done. Current maxTimeStamp: " + referenceTimeStamp + ". Equivalent date: " + new Date(referenceTimeStampByThread));

                                br.close();


                            }


                        } catch (IOException ee) {
                            // do nothing !!!
                        }

                        maxTimeStampByInputFileIndex.put(threadIndex
                                , referenceTimeStampByThread);
                    }

            );
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        return Collections.max(maxTimeStampByInputFileIndex.values());
    }

    @Override
    public void aggregate() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < csvInputFilePaths.size(); i++) {
            final int threadIndex = i;
            String filePath = csvInputFilePaths.get(i);

            executor.execute(() -> {
                try {
                    System.out.println("Aggregating ratings from the input file:" + filePath + " started. Sizes: Users, Products, Ratings: " + userIds.size() + "," + productIds.size());
                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath)));
                    SimpleAggregator simpleAggregator = new SimpleAggregator(true, br, userIds, productIds, ratingsByUserIdThenProductIds);
                    simpleAggregator.aggregate(referenceTimeStamp);
                    System.out.println("Aggregating ratings from the input file:" + filePath + " done. Sizes: Users, Products, Ratings: " + userIds.size() + "," + productIds.size());

                }
                catch (Exception e){
                    // do nothing
                    // do nothing
                }
                    }

            );
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
    }


    @Override
    public void writeIdLookUp(Map<String, Integer> idStringToIntMap, PrintWriter printWriter) {
        List<Map.Entry<String, Integer>> ratingsByUser=new ArrayList<>(idStringToIntMap.entrySet());
        ratingsByUser.sort(Comparator.comparingInt( v->v.getValue()));
        for(Map.Entry entry:ratingsByUser){
            printWriter.println(entry.getKey()+separator+entry.getValue());
        }
        printWriter.close();
    }
}


