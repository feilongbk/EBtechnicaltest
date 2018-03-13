import feilongbk.earlybirds.technicaltest.InputStreamGenerator;
import feilongbk.earlybirds.technicaltest.MultithreadingAggregationRunner;
import feilongbk.earlybirds.technicaltest.OutputStreamGenerator;
import feilongbk.earlybirds.technicaltest.SimpleAggregationRunner;
import org.junit.Test;

import java.util.*;

public class MultithreadingAggregationRunner_UT {


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
        inputFilePaths.add(inputFilePath);
        inputFilePaths.add(inputFilePath);
        long t_start = System.currentTimeMillis();
        long t_0 = t_start;
        InputStreamGenerator windowsWinStreamGenerator = new InputStreamGenerator() {
        };
        OutputStreamGenerator windowsOutputStreamGenerator = new OutputStreamGenerator() {
        };
        long maxTimeStamp = 1477353599713L;
        MultithreadingAggregationRunner aggregationRunner = new MultithreadingAggregationRunner(windowsWinStreamGenerator, windowsOutputStreamGenerator, inputFilePaths, aggRatingFilePath, lookUpUserFilePath, lookUpProductFilePath);

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
