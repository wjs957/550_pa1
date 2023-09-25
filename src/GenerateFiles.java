import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class GenerateFiles {
    public static void main(String[] args) {
        ConfigReader config = new ConfigReader("test_config.properties");
        String directoryPath = config.getProperty("test.download.path");
        int numFiles = 1000000; // Generate 1 million 1K files
//        int numFiles = 100;
        int fileSizeKB = 1024; // 1K
        generateFiles(directoryPath, "text_kb_%s.txt", numFiles, fileSizeKB);

        numFiles = 1000; // Generate 1000 1M files
        int fileSizeMB = 1024*1024; // 1M
        generateFiles(directoryPath, "text_mb_%s.txt", numFiles, fileSizeMB);

        numFiles = 10; // Generate 10 1G files
        int fileSizeGB = 1024*1024*1024; // 1G
        generateFiles(directoryPath, "binary_gb_%s.bin", numFiles, fileSizeGB);
    }

    private static void generateFiles(String directoryPath, String fileNameFormat, int numFiles, int fileSizeInKB) {
        File directory = new File(directoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            System.err.println("The specified directory does not exist or is not a directory");
            return;
        }

        byte[] data = new byte[fileSizeInKB];

        for (int i = 1; i <= numFiles; i++) {
            String fileName = String.format(fileNameFormat, i);
            File file = new File(directory, fileName);

            try (FileOutputStream outputStream = new FileOutputStream(file)) {
                outputStream.write(data);
                System.out.println("Generate file：" + file.getAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
