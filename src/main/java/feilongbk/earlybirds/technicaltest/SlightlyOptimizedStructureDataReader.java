package feilongbk.earlybirds.technicaltest;

import java.io.BufferedReader;
import java.io.IOException;

public class SlightlyOptimizedStructureDataReader implements DataReader {
    BufferedReader userIdBufferedReader;
    BufferedReader productIdBufferedReader;
    BufferedReader ratingBufferedReader;
    BufferedReader timeStampBufferedReader;

    public SlightlyOptimizedStructureDataReader(BufferedReader userIdBufferedReader, BufferedReader productIdBufferedReader, BufferedReader ratingBufferedReader, BufferedReader timeStampBufferedReader) {
        this.userIdBufferedReader = userIdBufferedReader;
        this.productIdBufferedReader = productIdBufferedReader;
        this.ratingBufferedReader = ratingBufferedReader;
        this.timeStampBufferedReader = timeStampBufferedReader;
    }

    @Override
    public InputRow readLine() throws IOException {
        return new InputRow(userIdBufferedReader.readLine(), productIdBufferedReader.readLine(), Float.parseFloat(ratingBufferedReader.readLine()), Long.parseLong(timeStampBufferedReader.readLine()));

    }

    @Override
    public long readTimeStamp() throws IOException {
        return Long.parseLong(timeStampBufferedReader.readLine());
    }

    public SlightlyOptimizedStructureDataReader() {
    }


    public void setTimeStampBufferedReader(BufferedReader timeStampBufferedReader) {
        this.timeStampBufferedReader = timeStampBufferedReader;
    }
}
