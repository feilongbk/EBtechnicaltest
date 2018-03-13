import feilongbk.earlybirds.technicaltest.InputStreamGenerator;
import feilongbk.earlybirds.technicaltest.OutputStreamGenerator;
import feilongbk.earlybirds.technicaltest.SimpleAggregationRunner;
import org.junit.Test;

import java.util.*;

public class SimpleAggregationRunner_UT {
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
        //inputFilePaths.add(inputFilePath);
        //

        long t_start = System.currentTimeMillis();
        long t_0 = t_start;
        InputStreamGenerator windowsWinStreamGenerator = new InputStreamGenerator() {
        };
        OutputStreamGenerator windoOutputStreamGenerator = new OutputStreamGenerator() {
        };
        long maxTimeStamp = 1477353599713L;
        SimpleAggregationRunner simpleAggregationRunner = new SimpleAggregationRunner(windowsWinStreamGenerator, windoOutputStreamGenerator, inputFilePaths, aggRatingFilePath, lookUpUserFilePath, lookUpProductFilePath);

        maxTimeStamp=simpleAggregationRunner.findReferenceTimeStamp();
        System.out.println("MaxTimeStamp: " + maxTimeStamp + ", date: " + new Date(maxTimeStamp));
        elapseTimeInMiliseconds.put(findMaxTimeStamp, System.currentTimeMillis() - t_0);
        t_0 = System.currentTimeMillis();
        System.out.println(findMaxTimeStamp + ": " + elapseTimeInMiliseconds.get(findMaxTimeStamp));
        simpleAggregationRunner.setReferenceTimeStamp(maxTimeStamp);
        simpleAggregationRunner.aggregate();
        elapseTimeInMiliseconds.put(aggregation, System.currentTimeMillis() - t_0);
        t_0 = System.currentTimeMillis();

        simpleAggregationRunner.exportDataToCsv();
        elapseTimeInMiliseconds.put(exporting, System.currentTimeMillis() - t_0);
        System.out.println("elapseTimeInMiliseconds: " + elapseTimeInMiliseconds);


    }
}
