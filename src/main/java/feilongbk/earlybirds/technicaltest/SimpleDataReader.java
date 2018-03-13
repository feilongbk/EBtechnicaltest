package feilongbk.earlybirds.technicaltest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/*
Read data from CSV file: standard format: userId, productId, rating, timeStampz
 */
public class SimpleDataReader implements DataReader {
    BufferedReader bufferedReader = null;
    String separator = ",";

    public SimpleDataReader(BufferedReader bufferedReader) {
        this.bufferedReader = bufferedReader;
    }



    @Override
    public InputRow readLine() throws IOException {
        String[] tempStringArray = bufferedReader.readLine().split(separator);
        return new InputRow(tempStringArray[0], tempStringArray[1], Float.parseFloat(tempStringArray[2]), Long.parseLong(tempStringArray[3]));
    }

    @Override
    public long readTimeStamp() throws IOException {
        return Long.parseLong(bufferedReader.readLine().split(separator)[3]);
    }

    public BufferedReader getBufferedReader() {
        return bufferedReader;
    }

    public void setBufferedReader(BufferedReader bufferedReader) {
        this.bufferedReader = bufferedReader;
    }


}


