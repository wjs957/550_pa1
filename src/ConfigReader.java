import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

public class ConfigReader {
    private Properties properties;

    public ConfigReader(String configFile) {
        String classPath = ConfigReader.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        properties = new Properties();
        try {
            FileInputStream input = new FileInputStream(classPath+configFile);
            properties.load(input);
            input.close();
        } catch (IOException e) {

            e.printStackTrace();
        }

    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public int getIntProperty(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
        return defaultValue;
    }

    public boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }


    public static void main(String[] args) {
        ConfigReader config = new ConfigReader("test_config.properties");
        // Example usage:
        String serverAddress = config.getProperty("index.server.address");
        int serverPort = config.getIntProperty("index.server.port", 8080);
        String fileName = config.getProperty("test.file.name");
        System.out.println("Server Address: " + serverAddress);
        System.out.println("Server Port: " + serverPort);
        System.out.println("fileName: " + fileName);
    }
}
