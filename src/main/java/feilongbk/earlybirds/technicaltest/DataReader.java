package feilongbk.earlybirds.technicaltest;

import java.io.IOException;

public interface DataReader {
    InputRow readLine() throws IOException;

    long readTimeStamp() throws IOException;
}
