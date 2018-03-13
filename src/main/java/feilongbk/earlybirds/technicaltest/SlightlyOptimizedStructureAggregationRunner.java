package feilongbk.earlybirds.technicaltest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.List;

public class SlightlyOptimizedStructureAggregationRunner extends SimpleAggregationRunner {
    public static final String userIdPrefix = "userId";
    public static final String productIdPrefix = "productId";
    public static final String ratingPrefix = "rating";
    public static final String timeStampPrefix = "timeStamp";
    public static final String dot=".";

    public SlightlyOptimizedStructureAggregationRunner(InputStreamGenerator inputStreamGenerator, OutputStreamGenerator outputStreamGenerator, List<String> csvInputFilePaths, String aggregateRatingFilePath, String lookUpUserFilePath, String lookUpProductFilePath) {
        super(inputStreamGenerator, outputStreamGenerator, csvInputFilePaths, aggregateRatingFilePath, lookUpUserFilePath, lookUpProductFilePath);
    }

    public SlightlyOptimizedStructureAggregationRunner(InputStreamGenerator inputStreamGenerator, OutputStreamGenerator outputStreamGenerator, List<String> csvInputFilePaths, long referenceTimeStamp, String aggregateRatingFilePath, String lookUpUserFilePath, String lookUpProductFilePath) {
        super(inputStreamGenerator, outputStreamGenerator, csvInputFilePaths, referenceTimeStamp, aggregateRatingFilePath, lookUpUserFilePath, lookUpProductFilePath);
    }

    @Override
    public long findReferenceTimeStamp() throws Exception {
        referenceTimeStamp = 0L;
        for (String filePath : csvInputFilePaths) {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath+dot+timeStampPrefix)));
            SlightlyOptimizedStructureDataReader dataReader = new SlightlyOptimizedStructureDataReader();
            dataReader.setTimeStampBufferedReader(br);
            System.out.println("Reading timeStamp from the input file:" + filePath + " started. Current maxTimeStamp: " + referenceTimeStamp + ". Equivalent date: " + new Date(referenceTimeStamp));
            try {
                long rowTimeStamp;
                while (true) {
                    if (referenceTimeStamp < (rowTimeStamp = dataReader.readTimeStamp())) {
                        referenceTimeStamp = rowTimeStamp;
                    }
                }
            } catch (Exception e) {
                System.out.println("Reading timeStamp from the input file:" + filePath + " done. Current maxTimeStamp: " + referenceTimeStamp + ". Equivalent date: " + new Date(referenceTimeStamp));
                br.close();
            }
        }
        return referenceTimeStamp;
    }

    @Override
    public void aggregate() throws Exception {
        for (String filePath : csvInputFilePaths) {
            System.out.println("Aggregating ratings from the input file:" + filePath + " started. Sizes: Users, Products, Ratings: " +userIds.size() + "," +productIds.size() );
            BufferedReader brUser = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath+dot+userIdPrefix)));
            BufferedReader brProduct = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath+dot+productIdPrefix)));
            BufferedReader brRating = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath+dot+ratingPrefix)));
            BufferedReader brTimeStamp = new BufferedReader(new InputStreamReader(inputStreamGenerator.generateFrom(filePath+dot+timeStampPrefix)));

            SimpleAggregator simpleAggregator = new SimpleAggregator(false,brUser, brProduct,brRating, brTimeStamp, userIds, productIds, ratingsByUserIdThenProductIds);
            simpleAggregator.aggregate(referenceTimeStamp);
            System.out.println("Aggregating ratings from the input file:" + filePath + " done. Sizes: Users, Products, Ratings: " +userIds.size() + "," +productIds.size() );
        }
    }
}
