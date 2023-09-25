import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FileUtils {
    private FileUtils(){}

    /** mark transfer success **/
    public static final byte[] successData = "success data mark".getBytes();

    public static void main(String[] args) {
        byte[] b = new byte[]{1};
        System.out.println(isEqualsByteArray(successData,b));
    }

    /**
     * compare byteArray equest successData
     * @param compareBuf
     * @param buf
     * @return
     */
    public static boolean isEqualsByteArray(byte[] compareBuf,byte[] buf){
        if (buf == null || buf.length == 0)
            return false;

        boolean flag = true;
        if(buf.length == compareBuf.length){
            for (int i = 0; i < buf.length; i++) {
                if(buf[i] != compareBuf[i]){
                    flag = false;
                    break;
                }
            }
        }else
            return false;
        return flag;
    }

    /**
     * compare byteArray equest successData
     * @param compareBuf src
     * @param buf target
     * @return
     */
    public static boolean isEqualsByteArray(byte[] compareBuf,byte[] buf,int len){
        if (buf == null || buf.length == 0 || buf.length < len || compareBuf.length < len)
            return false;
        boolean flag = true;
        int innerMinLen = Math.min(compareBuf.length, len);
        //if(buf.length == compareBuf.length){
        for (int i = 0; i < innerMinLen; i++) {
            if(buf[i] != compareBuf[i]){
                flag = false;
                break;
            }
        }
        //}else
        //	return false;
        return flag;
    }

    public static DirectoryEntity listFilesInDirectoryOrFile(String filePath) {
        List<String> fileNames = new ArrayList<>();
        File directory = new File(filePath);
        DirectoryEntity directoryEntity =null;
        if (directory.exists()) {
            directoryEntity = new DirectoryEntity();
            if(directory.isDirectory()){
                directoryEntity.setFileDirectory(directory.getPath()+ File.separator);
                File[] files = directory.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (file.isFile()) {
                            fileNames.add(file.getName());
                        }
                    }
                }
            }
            if(directory.isFile()){
                directoryEntity.setFileDirectory(directory.getParent() +File.separator);
                fileNames.add(directory.getName());
            }
            directoryEntity.setFileNames(fileNames);
        }

        return directoryEntity;
    }

    public static void readAndOutputFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static long getFileSizeInBytes(String filePath) {
        File file = new File(filePath);

        // 检查文件是否存在
        if (!file.exists() || !file.isFile()) {
            return -1;
        }

        // 获取文件的字节数
        return file.length();
    }

    public static Path getUserDownloadPath() throws IOException {
        // 获取当前用户的 Downloads 目录路径
        String userHome = System.getProperty("user.home");
        Path downloadsPath = Paths.get(userHome, "Downloads");

        // 创建 Downloads 目录（如果不存在）
        if (!Files.exists(downloadsPath)) {
            Files.createDirectories(downloadsPath);
        }
        return downloadsPath;
    }
    public static class DirectoryEntity{
        private List<String> fileNames;
        private String fileDirectory;

        public List<String> getFileNames() {
            return fileNames;
        }

        public void setFileNames(List<String> fileNames) {
            this.fileNames = fileNames;
        }

        public String getFileDirectory() {
            return fileDirectory;
        }

        public void setFileDirectory(String fileDirectory) {
            this.fileDirectory = fileDirectory;
        }
    }
}