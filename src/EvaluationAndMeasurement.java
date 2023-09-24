import java.io.*;
import java.net.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EvaluationAndMeasurement {

    private static final String LOG_FILE_NAME = "search_log.log";
    private static final int NUM_THREADS = 10;
    private static final int NUM_QUERIES = 1000;

    private static final String TEST_FILE_NAME = "makefile";

    public static void main(String[] args) {
        EvaluationAndMeasurement evaluation = new EvaluationAndMeasurement();
        evaluation.runQueries();
    }

    public void runQueries() {
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);

        ConfigReader config = new ConfigReader("test_config.properties");
        try {
            BufferedWriter logWriter = new BufferedWriter(new FileWriter(LOG_FILE_NAME));

            String serverAddress = config.getProperty("index.server.address");

            String serverPort = config.getProperty("index.server.port");

            final String finalServerAddress = serverAddress;
            final String finalServerPort = serverPort;
            for (int i = 0; i < NUM_THREADS; i++) {

                threadPool.execute(() -> {
                    try {
                        for (int j = 0; j < NUM_QUERIES; j++) {
                            long startTime = System.currentTimeMillis();
                            performSocketQuery(finalServerAddress,finalServerPort);
                            long endTime = System.currentTimeMillis();
                            long elapsedTime = endTime - startTime;

                            // Log the search information
                            String logEntry = String.format(
                                    "File: %s, Start Time: %s, End Time: %s, Elapsed Time (ms): %d%n",
                                    config.getProperty("test.file.name"), new Date(startTime), new Date(endTime), elapsedTime
                            );
                            synchronized (logWriter) {
                                logWriter.write(logEntry);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }

            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logWriter.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void performSocketQuery(String serverAddress,String serverPort)  {
        Socket peerClientSocket =null;
        ObjectOutputStream out=null;
        ObjectInputStream in=null;
        try{
            peerClientSocket = new Socket(serverAddress, Integer.parseInt(serverPort));



            out = new ObjectOutputStream(peerClientSocket.getOutputStream());
            out.flush();

            in = new ObjectInputStream(peerClientSocket.getInputStream());
            IndexResponse indexServerResponse = (IndexResponse) in.readObject();
            if(!indexServerResponse.isSuc()){
                System.out.println("Connection failure.Address: "+serverAddress+",Port:"+serverPort);
                System.exit(0);
            }

            System.out.println("The PEER has established a connection with server "+serverAddress+":"+serverPort+" .");

            IndexRequest indexRequest = new IndexRequest();
            indexRequest.setRequestType(RequestTypeEnum.LOOKUP.getCode());
            indexRequest.setIndexSearch(new IndexRequest.IndexSearch());
            indexRequest.getIndexSearch().setFileName(TEST_FILE_NAME);

            out.writeObject(indexRequest);
            indexServerResponse = (IndexResponse) in.readObject();

            if (indexServerResponse.isSuc()) {
                // The response result of the index service
                HashMap<Integer, IndexResponse.LookupItem> lookupMap = indexServerResponse.getData().getPeerAndIpMapping();

                // The information about all the queried files is displayed
                if (lookupMap != null) {
                    IndexResponse.LookupItem lookupItem;
                    for (Map.Entry<Integer, IndexResponse.LookupItem> entry : lookupMap.entrySet()) {
                        lookupItem = entry.getValue();
                        System.out.println("\nNumber: " + entry.getKey() + " , Peer ID:" + lookupItem.getPeerId() + ", Host Address:" + lookupItem.getFileServerAddress() + ":" + lookupItem.getFileServerPort());
                    }
                } else {
                    System.err.println("File retrieval failed, failure message: The host list is empty");
                }
            }
        }catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
        }finally {
            try {
                if (out != null){
                    out.close();
                }
                if (in != null){
                    in.close();
                }
                if (peerClientSocket != null){
                    peerClientSocket.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
