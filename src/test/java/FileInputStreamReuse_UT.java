import org.junit.Test;

import java.io.*;

public class FileInputStreamReuse_UT {

    @Test
    public void fileInputStreamIsReusable() throws Exception {
        System.out.println(System.getenv("USERNAME"));
        String inputFileName = "xag.csv";
       // String inputFileName = "abc.txt";
        String inputFilePath = "C:/Users/" + System.getenv("USERNAME") + "/Desktop/TechnicalTest/" + inputFileName;
        File inputFile=new File(inputFilePath);
        FileInputStream fileInputStream1=new FileInputStream(inputFile);
        System.out.println(fileInputStream1);
        BufferedReader br1=new BufferedReader(new InputStreamReader(fileInputStream1));
        System.out.println(br1.readLine());
        FileInputStream fileInputStream2=new FileInputStream(inputFile);
        BufferedReader br2=new BufferedReader(new InputStreamReader(fileInputStream1));
        System.out.println(br2.readLine());
    }


}