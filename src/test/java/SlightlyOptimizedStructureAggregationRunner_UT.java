import feilongbk.earlybirds.technicaltest.*;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.*;

// Splitting column saves time in splitting, however, the reading operations with 4 buffered readers cause performance issues
// This solution is rejected
public class SlightlyOptimizedStructureAggregationRunner_UT {
    private boolean inputPrepared = false;
    String separator=",";

    @Test
    public void prepareInput() throws Exception {
        if (inputPrepared) {
            String inputFileName = "xag.csv";
            String testDirectoryPath = "C:/Users/" + System.getenv("USERNAME") + "/Desktop/TechnicalTest";
            String inputFilePath = testDirectoryPath + "/" + inputFileName;
            String userIdColumnFilePath = testDirectoryPath + "/" + inputFileName+SlightlyOptimizedStructureAggregationRunner.dot + SlightlyOptimizedStructureAggregationRunner.userIdPrefix ;
            String productIdColumnFilePath = testDirectoryPath + "/" + inputFileName+SlightlyOptimizedStructureAggregationRunner.dot + SlightlyOptimizedStructureAggregationRunner.productIdPrefix ;
            String ratingColumnFilePath = testDirectoryPath + "/" + inputFileName+SlightlyOptimizedStructureAggregationRunner.dot + SlightlyOptimizedStructureAggregationRunner.ratingPrefix ;
            String timeStampColumnFilePath = testDirectoryPath + "/" +  inputFileName+SlightlyOptimizedStructureAggregationRunner.dot + SlightlyOptimizedStructureAggregationRunner.timeStampPrefix;

            PrintWriter userIdPrintWriter = new PrintWriter(new File(userIdColumnFilePath));
            PrintWriter productIdrintWriter = new PrintWriter(new File(productIdColumnFilePath));
            PrintWriter ratingPrintWriter = new PrintWriter(new File(ratingColumnFilePath));
            PrintWriter timeStampPrintWriter = new PrintWriter(new File(timeStampColumnFilePath));
            BufferedReader inputFileBufferedReader=new BufferedReader(new FileReader(new File(inputFilePath)));

          try{

              while (true){
                  String[] tempStringArray = inputFileBufferedReader.readLine().split(separator);
                  userIdPrintWriter.println(tempStringArray[0]);
                  productIdrintWriter.println(tempStringArray[1]);
                  ratingPrintWriter.println(tempStringArray[2]);
                  timeStampPrintWriter.println(tempStringArray[3]);
              }
          }
          catch (Exception e){
              /// Do nothing
             // e.printStackTrace();
          }
            userIdPrintWriter.close();
            productIdrintWriter.close();
            ratingPrintWriter.close();
            timeStampPrintWriter.close();
            inputFileBufferedReader.close();


        }

    }


    @Test
    public void Test() throws Exception {
        String findMaxTimeStamp = "Finding Max TimeStamp";
        String aggregation = "Aggregation";
        String exporting = "Exporting";
        Map<String, Long> elapseTimeInMiliseconds = new HashMap<>();
        String inputFileName = "xag.csv";
        String testDirectoryPath = "C:/Users/" + System.getenv("USERNAME") + "/Desktop/TechnicalTest";
        String inputFilePath = testDirectoryPath + "/" + inputFileName;
        String aggRatingFilePath = testDirectoryPath + "/" + "aggratings.csv";
        String lookUpUserFilePath = testDirectoryPath + "/" + "lookupuser.csv";
        String lookUpProductFilePath = testDirectoryPath + "/" + "lookup_product.csv";
        List<String> inputFilePaths = new ArrayList<>();
        // Duplicate the number of input CSV File to simulate the processing of many files
        inputFilePaths.add(inputFilePath);
       // inputFilePaths.add(inputFilePath);
        //

        long t_start = System.currentTimeMillis();
        long t_0 = t_start;
        InputStreamGenerator windowsWinStreamGenerator = new InputStreamGenerator() {
        };
        OutputStreamGenerator windoOutputStreamGenerator = new OutputStreamGenerator() {
        };
        long maxTimeStamp = 1477353599713L;
        SlightlyOptimizedStructureAggregationRunner aggregationRunner = new SlightlyOptimizedStructureAggregationRunner(windowsWinStreamGenerator, windoOutputStreamGenerator, inputFilePaths, aggRatingFilePath, lookUpUserFilePath, lookUpProductFilePath);

        maxTimeStamp=aggregationRunner.findReferenceTimeStamp();
        System.out.println("MaxTimeStamp: " + maxTimeStamp + ", date: " + new Date(maxTimeStamp));
        elapseTimeInMiliseconds.put(findMaxTimeStamp, System.currentTimeMillis() - t_0);
        t_0 = System.currentTimeMillis();
        System.out.println(findMaxTimeStamp + ": " + elapseTimeInMiliseconds.get(findMaxTimeStamp));
        aggregationRunner.setReferenceTimeStamp(maxTimeStamp);
        aggregationRunner.aggregate();
        elapseTimeInMiliseconds.put(aggregation, System.currentTimeMillis() - t_0);
        t_0 = System.currentTimeMillis();

        aggregationRunner.exportDataToCsv();
        elapseTimeInMiliseconds.put(exporting, System.currentTimeMillis() - t_0);
        System.out.println("elapseTimeInMiliseconds: " + elapseTimeInMiliseconds);


    }

}
