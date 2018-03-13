package feilongbk.earlybirds.technicaltest;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public interface InputStreamGenerator {
    default FileInputStream generateFrom(String filePath) throws IOException {
    return new FileInputStream(new File(filePath));
    }
}
