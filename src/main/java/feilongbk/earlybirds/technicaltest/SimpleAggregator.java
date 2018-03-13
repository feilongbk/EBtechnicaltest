package feilongbk.earlybirds.technicaltest;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleAggregator implements Aggregator {
    public static final int maxDistanceOnDates = 1000;
    public static final long numberOfMillisecondsByDay=86400000;
    public static final float ratingThreshold=0.01F;
    public static final float  penaltyMultiplier = 0.95F;
    DataReader dataReader;
    Map<String, Integer> userIds = new LinkedHashMap<>();
    Map<String, Integer> productIds = new LinkedHashMap<>();
    Map<Integer, Map<Integer, Float>> ratingsByUserIdThenProductIds = new HashMap<>();
    //Map<Integer,Float> penaltyFactorBDistanceOnDates=new HashMap<>();
    public static final float[] penaltyFactorBDistanceOnDates = new float[maxDistanceOnDates];

    private boolean threadSafe=false;
    //
    {
        penaltyFactorBDistanceOnDates[0] = 1;
        for (int i = 1; i < maxDistanceOnDates; i++) {
            penaltyFactorBDistanceOnDates[i] = penaltyFactorBDistanceOnDates[i - 1] * penaltyMultiplier;
        }
    }

    public SimpleAggregator(BufferedReader inputFileBufferedReader) throws Exception {
        dataReader = new SimpleDataReader(inputFileBufferedReader);
    }

    public SimpleAggregator(boolean threadSafe, BufferedReader inputFileBufferedReader, Map<String, Integer> userIds, Map<String, Integer> productIds, Map<Integer, Map<Integer, Float>> ratingsByUserIdThenProductIds) {
        this.threadSafe=threadSafe;
        this.userIds = userIds;
        this.productIds = productIds;
        this.ratingsByUserIdThenProductIds = ratingsByUserIdThenProductIds;
        dataReader = new SimpleDataReader(inputFileBufferedReader);
    }

    public SimpleAggregator(boolean threadSafe, BufferedReader userIdBufferedReader, BufferedReader productIdBufferedReader, BufferedReader ratingBufferedReader, BufferedReader timeStampBufferedReader, Map<String, Integer> userIds, Map<String, Integer> productIds, Map<Integer, Map<Integer, Float>> ratingsByUserIdThenProductIds) {
        this.threadSafe=threadSafe;
        this.userIds = userIds;
        this.productIds = productIds;
        this.ratingsByUserIdThenProductIds = ratingsByUserIdThenProductIds;
        dataReader = new SlightlyOptimizedStructureDataReader( userIdBufferedReader,  productIdBufferedReader,  ratingBufferedReader,  timeStampBufferedReader) ;
        }




    // UserIdAsInteger starts by 0
    public void aggregate(long referenceTimeStamp) {
        try {
            while (true) {
                InputRow row = dataReader.readLine();
                Integer userIdAsInteger = userIds.get(row.getUserId());
                if (userIdAsInteger == null) {
                    if(threadSafe){
                        synchronized (userIds){
                        userIdAsInteger = userIds.size();
                        userIds.put(row.getUserId(), userIdAsInteger); }
                    }
                    else {
                        userIdAsInteger = userIds.size();
                        userIds.put(row.getUserId(), userIdAsInteger);
                    }

                }
                Integer productIdAsInteger = productIds.get(row.getProductId());
                if (productIdAsInteger == null) {

                    if(threadSafe){
                        synchronized (productIds){
                            productIdAsInteger = productIds.size();
                            productIds.put(row.getProductId(), productIdAsInteger); }
                    }
                    else {
                        productIdAsInteger = productIds.size();
                        productIds.put(row.getProductId(), productIdAsInteger);
                    }


                }
            int distanceOnDate= (int) Math.floorDiv((referenceTimeStamp-row.getTimeStamp()),numberOfMillisecondsByDay);

                float penalizedRatingContribution;

            if(distanceOnDate>0&&distanceOnDate<=maxDistanceOnDates) {
                penalizedRatingContribution=row.getRating()*penaltyFactorBDistanceOnDates[distanceOnDate];
            }
            else {
                penalizedRatingContribution=row.getRating();
            }
            if(penalizedRatingContribution>=ratingThreshold){
                Float actualRatingSum;
                Map<Integer, Float> productRatings=ratingsByUserIdThenProductIds.get(userIdAsInteger);
                if(productRatings!=null){
                    if((actualRatingSum=productRatings.get(productIdAsInteger))!=null){
                        productRatings.put(productIdAsInteger,actualRatingSum+penalizedRatingContribution) ;
                    }
                    else{
                        productRatings.put(productIdAsInteger,penalizedRatingContribution) ;
                    }
                }
                else {
                    if(threadSafe) {
                        productRatings = new ConcurrentHashMap<>();
                    }
                    else {
                        productRatings = new HashMap<>();
                    }
                    productRatings.put(productIdAsInteger,penalizedRatingContribution);
                    ratingsByUserIdThenProductIds.put(userIdAsInteger, productRatings);
                 }
            }
            }
        } catch (Exception e) {
            //Ignore this null pointer exception

        }
    }


    private void upDateIdAsIntegerThreadSafe(Map<String, Integer> idMap, String idString){


    }

    private void upDateIdAsIntegerNonThreadSafe(Map<String, Integer> idMap, String idString){



    }

}
