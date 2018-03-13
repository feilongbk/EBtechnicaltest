package feilongbk.earlybirds.technicaltest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;

public class SimpleAggregationRunner {
    Map<String, Integer> userIds = new LinkedHashMap<>();
    Map<String, Integer> productIds = new LinkedHashMap<>();
    Map<Integer, Map<Integer, Float>> ratingsByUserIdThenProductIds = new HashMap<>();
    String separator=",";
    InputStreamGenerator inputStreamGenerator;
    OutputStreamGenerator outputStreamGenerator;
    List<String> csvInputFilePaths;
    boolean autoFindReferenceTimeStamp = true;
    long referenceTimeStamp;
    String aggregateRatingFilePath;
    String lookUpUserFilePath;
    String lookUpProductFilePath;

    public SimpleAggregationRunner(InputStreamGenerator inputStreamGenerator, OutputStreamGenerator outputStreamGenerator,List<String> csvInputFilePaths, String aggregateRatingFilePath, String lookUpUserFilePath, String lookUpProductFilePath) {
        this.csvInputFilePaths = csvInputFilePaths;
        this.aggregateRatingFilePath = aggregateRatingFilePath;
        this.lookUpUserFilePath = lookUpUserFilePath;
        this.lookUpProductFilePath = lookUpProductFilePath;
        this.inputStreamGenerator = inputStreamGenerator;
        this.outputStreamGenerator=outputStreamGenerator;
    }

    public SimpleAggregationRunner(InputStreamGenerator inputStreamGenerator,OutputStreamGenerator outputStreamGenerator, List<String> csvInputFilePaths, long referenceTimeStamp, String aggregateRatingFilePath, String lookUpUserFilePath, String lookUpProductFilePath) {
        this.csvInputFilePaths = csvInputFilePaths;
        this.referenceTimeStamp = referenceTimeStamp;
        this.aggregateRatingFilePath = aggregateRatingFilePath;
        this.lookUpUserFilePath = lookUpUserFilePath;
        this.lookUpProductFilePath = lookUpProductFilePath;
        this.inputStreamGenerator = inputStreamGenerator;
        this.outputStreamGenerator=outputStreamGenerator;
    }


    public long findReferenceTimeStamp() throws Exception {
        referenceTimeStamp = 0L;
        for (String filePath : csvInputFilePaths) {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath)));
            DataReader dataReader = new SimpleDataReader(br);
            System.out.println("Reading timeStamp from the input file:" + filePath + " started. Current maxTimeStamp: " + referenceTimeStamp + ". Equivalent date: " + new Date(referenceTimeStamp));
            try {
                long rowTimeStamp;
                while (true) {
                    if (referenceTimeStamp < (rowTimeStamp = dataReader.readTimeStamp())) {
                        referenceTimeStamp = rowTimeStamp;
                    }
                }
            } catch (NullPointerException e) {
                System.out.println("Reading timeStamp from the input file:" + filePath + " done. Current maxTimeStamp: " + referenceTimeStamp + ". Equivalent date: " + new Date(referenceTimeStamp));
                br.close();
            }
        }
        return referenceTimeStamp;
    }

    public void aggregate() throws Exception {
        for (String filePath : csvInputFilePaths) {
            System.out.println("Aggregating ratings from the input file:" + filePath + " started. Sizes: Users, Products, Ratings: " +userIds.size() + "," +productIds.size() );
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath)));
            SimpleAggregator simpleAggregator = new SimpleAggregator(false,br, userIds, productIds, ratingsByUserIdThenProductIds);
            simpleAggregator.aggregate(referenceTimeStamp);
            System.out.println("Aggregating ratings from the input file:" + filePath + " done. Sizes: Users, Products, Ratings: " +userIds.size() + "," +productIds.size() );
        }
    }

    public void exportDataToCsv()throws Exception {
        PrintWriter printWriterUserLookUp=new PrintWriter(outputStreamGenerator.generateFrom(lookUpUserFilePath));
        writeIdLookUp(userIds,printWriterUserLookUp);
        userIds.clear();
        PrintWriter printWriterProductLookUp=new PrintWriter(outputStreamGenerator.generateFrom(lookUpProductFilePath));
        writeIdLookUp(productIds,printWriterProductLookUp);
        userIds.clear();

        PrintWriter printWriterAggRatings=new PrintWriter(outputStreamGenerator.generateFrom(aggregateRatingFilePath));
        writeAggRatings(ratingsByUserIdThenProductIds, printWriterAggRatings);


    }


    public void writeIdLookUp(Map<String,Integer> idStringToIntMap, PrintWriter printWriter){
    for(Map.Entry entry:idStringToIntMap.entrySet()){
        printWriter.println(entry.getKey()+separator+entry.getValue());
    }
        printWriter.close();
    }


    public void writeAggRatings(Map<Integer,Map<Integer,Float>> ratingsByUserIdThenProductIds, PrintWriter printWriter){
       List<Map.Entry<Integer,Map<Integer,Float>>> ratingsByUser=new ArrayList<>(ratingsByUserIdThenProductIds.entrySet());
        ratingsByUser.sort(Comparator.comparingInt( v->v.getKey()));
        for(Map.Entry<Integer,Map<Integer,Float>> mapEntry:ratingsByUser){
            List<Map.Entry<Integer,Float>> ratingsByProduct=new ArrayList<>(mapEntry.getValue().entrySet());
            ratingsByProduct.sort(Comparator.comparingInt( v->v.getKey()));
            ratingsByProduct.forEach(v->printWriter.println(mapEntry.getKey()+separator+ v.getKey()+separator+v.getValue()));
        }
    }







    public Map<String, Integer> getUserIds() {
        return userIds;
    }

    public void setUserIds(Map<String, Integer> userIds) {
        this.userIds = userIds;
    }

    public Map<String, Integer> getProductIds() {
        return productIds;
    }

    public void setProductIds(Map<String, Integer> productIds) {
        this.productIds = productIds;
    }

    public Map<Integer, Map<Integer, Float>> getRatingsByUserIdThenProductIds() {
        return ratingsByUserIdThenProductIds;
    }

    public void setRatingsByUserIdThenProductIds(Map<Integer, Map<Integer, Float>> ratingsByUserIdThenProductIds) {
        this.ratingsByUserIdThenProductIds = ratingsByUserIdThenProductIds;
    }

    public InputStreamGenerator getInputStreamGenerator() {
        return inputStreamGenerator;
    }

    public void setInputStreamGenerator(InputStreamGenerator inputStreamGenerator) {
        this.inputStreamGenerator = inputStreamGenerator;
    }

    public List<String> getCsvInputFilePaths() {
        return csvInputFilePaths;
    }

    public void setCsvInputFilePaths(List<String> csvInputFilePaths) {
        this.csvInputFilePaths = csvInputFilePaths;
    }

    public boolean isAutoFindReferenceTimeStamp() {
        return autoFindReferenceTimeStamp;
    }

    public void setAutoFindReferenceTimeStamp(boolean autoFindReferenceTimeStamp) {
        this.autoFindReferenceTimeStamp = autoFindReferenceTimeStamp;
    }

    public long getReferenceTimeStamp() {
        return referenceTimeStamp;
    }

    public void setReferenceTimeStamp(long referenceTimeStamp) {
        this.referenceTimeStamp = referenceTimeStamp;
    }

    public String getAggregateRatingFilePath() {
        return aggregateRatingFilePath;
    }

    public void setAggregateRatingFilePath(String aggregateRatingFilePath) {
        this.aggregateRatingFilePath = aggregateRatingFilePath;
    }

    public String getLookUpUserFilePath() {
        return lookUpUserFilePath;
    }

    public void setLookUpUserFilePath(String lookUpUserFilePath) {
        this.lookUpUserFilePath = lookUpUserFilePath;
    }

    public String getLookUpProductFilePath() {
        return lookUpProductFilePath;
    }

    public void setLookUpProductFilePath(String lookUpProductFilePath) {
        this.lookUpProductFilePath = lookUpProductFilePath;
    }
}
