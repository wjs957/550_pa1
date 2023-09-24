import java.io.*;
import java.net.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EvaluationAndMeasurement {

    private static final String LOG_FILE_NAME = "search_log.txt";
    private static final int NUM_THREADS = 10;
    private static final int NUM_QUERIES = 1000;

    public static void main(String[] args) {
        EvaluationAndMeasurement evaluation = new EvaluationAndMeasurement();
        evaluation.runQueries();
    }

    public void runQueries() {
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);
        try {
            BufferedWriter logWriter = new BufferedWriter(new FileWriter(LOG_FILE_NAME));

            for (int i = 0; i < NUM_THREADS; i++) {
                threadPool.execute(() -> {
                    try {
                        for (int j = 0; j < NUM_QUERIES; j++) {
                            long startTime = System.currentTimeMillis();
                            String queryResult = performSocketQuery();
                            long endTime = System.currentTimeMillis();
                            long elapsedTime = endTime - startTime;

                            // Log the search information
                            String logEntry = String.format(
                                    "File: %s, Start Time: %s, End Time: %s, Elapsed Time (ms): %d%n",
                                    queryResult, new Date(startTime), new Date(endTime), elapsedTime
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

    // Replace this method with your actual socket query logic
    private String performSocketQuery() throws IOException, ClassNotFoundException {
        Socket peerClientSocket = null;
        BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        IndexRequest indexRequest;
        IndexResponse indexServerResponse;


        System.out.print("Enter Server IP Address (The default address is 127.0.0.1):");
        String serverAddress = input.readLine();

        if(serverAddress.trim().length() == 0 || "\n".equals(serverAddress)) {
            serverAddress = DEFAULT_INDEX_SERVER_HOST;
        }

        if(!IPAddressValidator.isValidIP(serverAddress)) {
            System.out.println("Invalid Server IP Address.");
            System.exit(0);
        }

        System.out.print("Enter Server PORT (The default PORT is 8080):");
        String serverPort = input.readLine();
        if(serverPort.trim().length() == 0 || "\n".equals(serverPort)) {
            serverPort = DEFAULT_INDEX_SERVER_PORT;
        }

        peerClientSocket = new Socket(serverAddress, Integer.parseInt(serverPort));

        out = new ObjectOutputStream(peerClientSocket.getOutputStream());
        out.flush();

        in = new ObjectInputStream(peerClientSocket.getInputStream());


        indexServerResponse = (IndexResponse) in.readObject();
        if(!indexServerResponse.isSuc()){
            System.out.println("Connection failure.Address: "+serverAddress+",Port:"+serverPort);
            System.exit(0);
        }

        System.out.println("The PEER has established a connection with server "+serverAddress+":"+serverPort+" .");


        indexRequest = new IndexRequest();
        indexRequest.setRequestType(RequestTypeEnum.LOOKUP.getCode());
        indexRequest.setIndexSearch(new IndexRequest.IndexSearch());
        indexRequest.getIndexSearch().setFileName(fileName);

        out.writeObject(indexRequest);
        indexServerResponse = (IndexResponse) in.readObject();
        String downloadFileLocation =null;
        if (indexServerResponse.isSuc()) {
            // The response result of the index service
            HashMap<Integer, IndexResponse.LookupItem> lookupMap = indexServerResponse.getData().getPeerAndIpMapping();

            // The information about all the queried files is displayed
            IndexResponse.LookupItem firstItem = null;
            if (lookupMap != null) {
                IndexResponse.LookupItem lookupItem;
                for (Map.Entry<Integer, IndexResponse.LookupItem> entry : lookupMap.entrySet()) {
                    lookupItem = entry.getValue();
                    System.out.println("\nNumber: " + entry.getKey() + " , Peer ID:" + lookupItem.getPeerId() + ", Host Address:" + lookupItem.getFileServerAddress() + ":" + lookupItem.getFileServerPort());
                    if (firstItem == null) {
                        firstItem = entry.getValue();
                    }
                }
            } else {
                System.err.println("File retrieval failed, failure message: The host list is empty");
                break;
            }
        }

        return "SampleQueryResult";
    }
}
