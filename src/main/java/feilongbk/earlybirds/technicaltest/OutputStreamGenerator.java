package feilongbk.earlybirds.technicaltest;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface OutputStreamGenerator {
    default FileOutputStream generateFrom(String filePath) throws Exception {
        // delete the existing file
        try {
            Path fileToDeletePath = Paths.get(filePath);
            Files.delete(fileToDeletePath);
        }catch (Exception e){
            // do nothing
        }
        return new FileOutputStream(new File(filePath));
    }
}
