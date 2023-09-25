import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class EvaluationAndMeasurement {


    private static final int NUM_THREADS = 10;
    private static final int NUM_QUERIES = 1000;
    private static final Map<Integer,String> TYPE_LOG_MAP = new HashMap<>();
    static {
        TYPE_LOG_MAP.put(0,"search_log.log");
        TYPE_LOG_MAP.put(1,"search_and_download_1M_1KB.log");
        TYPE_LOG_MAP.put(2,"search_and_download_1K_1MB.log");
        TYPE_LOG_MAP.put(3,"search_and_download_10_1GB.log");
    }

    private final Lock lock = new ReentrantLock();

    public static void main(String[] args) {
        EvaluationAndMeasurement evaluation = new EvaluationAndMeasurement();
        
        evaluation.runQueries(0);
       
       
        evaluation.runQueries(1);
        evaluation.runQueries(2);
        evaluation.runQueries(3);
    }

    public void runQueries(int type) {
        ExecutorService threadPool = Executors.newFixedThreadPool(NUM_THREADS);
        String logFile = TYPE_LOG_MAP.get(type);
        ConfigReader config = new ConfigReader("test_config.properties");
        try {
            BufferedWriter logWriter = new BufferedWriter(new FileWriter(logFile));
            String targetFile0 = config.getProperty("test.file.name");
            String targetFile1 = config.getProperty("test.1M.1KB.text.file.name");
            String targetFile2 = config.getProperty("test.1K.1MB.text.file.name");
            String targetFile3 = config.getProperty("test.10.1GB.binary.file.name");
            String downloadPath =  config.getProperty("test.download.path");
            String serverAddress = config.getProperty("index.server.address");
            String serverPort = config.getProperty("index.server.port");

            int kbFileSize = config.getIntProperty("test.1M.1KB.text.file.size",1000000);
            int mbFileSize = config.getIntProperty("test.1K.1MB.text.file.size",1000);

            final String finalServerAddress = serverAddress;
            final String finalServerPort = serverPort;

            int range = 0;
            int remainder =0;

            int start = 1; // begin number
            int end = 0;   // end number
            if(type==0){
                synchronized (logWriter) {
                    logWriter.write("File,LookUp Start Time,LookUp End Time,LookUp Elapsed Time (ms)\n");
                }
            }else if(type==1){
                synchronized (logWriter) {
                    logWriter.write("File,LookUp Start Time,LookUp End Time,LookUp Elapsed Time (ms),Download Start Time,Download End Time,Download Elapsed Time (ms),Total Elapsed Time (ms)\n");
                }

                range = kbFileSize / NUM_THREADS;
                remainder = kbFileSize % NUM_THREADS;
            }else if(type==2){
                synchronized (logWriter) {
                    logWriter.write("File,LookUp Start Time,LookUp End Time,LookUp Elapsed Time (ms),Download Start Time,Download End Time,Download Elapsed Time (ms),Total Elapsed Time (ms)\n");
                }
                range = mbFileSize / NUM_THREADS;
                remainder = mbFileSize % NUM_THREADS;
            }else{
                synchronized (logWriter) {
                    logWriter.write("File,LookUp Start Time,LookUp End Time,LookUp Elapsed Time (ms),Download Start Time,Download End Time,Download Elapsed Time (ms),Total Elapsed Time (ms)\n");
                }
            }

            for (int i = 0; i < NUM_THREADS; i++) {
                end = start + range - 1;

                // 如果有余数，将余数分配给前面的线程
                if (remainder > 0) {
                    end++;
                    remainder--;
                }
                final int threadStart = start;
                final int threadEnd = end;

                int finalI = i+1;

                threadPool.execute(() -> {
                    try {
                        String logEntry;
                        MeasurementResult measurementResult;
                        String reqFileName;
                        if(type==0){

                            for (int j = 0; j < NUM_QUERIES; j++) {

                                measurementResult = performSocketQuery(finalServerAddress,finalServerPort,targetFile0,type,null);
                                long startTime = measurementResult.getCallLookUpStartTime();
                                long endTime = measurementResult.getCallLookUpStopTime();
                                long elapsedTime = measurementResult.getCallLookUpElapsedTime();

                                logEntry = String.format(
                                        "%s,%d,%d,%d%n",
                                        targetFile0, startTime, endTime, elapsedTime
                                );

                                synchronized (logWriter) {
                                    logWriter.write(logEntry);
                                }
                            }
                        }else if(type==1){

                            for (int j = threadStart; j <= threadEnd; j++) {
                                reqFileName = String.format(targetFile1,j);
                                measurementResult = performSocketQuery(finalServerAddress,finalServerPort,reqFileName,type,downloadPath);
                                long startTime = measurementResult.getCallLookUpStartTime();
                                long endTime = measurementResult.getCallLookUpStopTime();
                                long elapsedTime = measurementResult.getCallLookUpElapsedTime();
                                logEntry = String.format(
                                        "%s,%d,%d,%d,%d,%d,%d,%d%n",
                                        reqFileName, startTime, endTime, elapsedTime
                                        , measurementResult.getDownloadStartTime(), measurementResult.getDownloadStopTime()
                                        , measurementResult.getDownloadElapsedTime(),elapsedTime + measurementResult.getDownloadElapsedTime()
                                );
                                synchronized (logWriter) {
                                    logWriter.write(logEntry);
                                }
                            }
                        }else if(type==2){

                            for (int j = threadStart; j <= threadEnd; j++) {
                                reqFileName = String.format(targetFile2,j);
                                measurementResult = performSocketQuery(finalServerAddress,finalServerPort,reqFileName,type,downloadPath);
                                long startTime = measurementResult.getCallLookUpStartTime();
                                long endTime = measurementResult.getCallLookUpStopTime();
                                long elapsedTime = measurementResult.getCallLookUpElapsedTime();
                                logEntry = String.format(
                                        "%s,%d,%d,%d,%d,%d,%d,%d%n",
                                        reqFileName, startTime, endTime, elapsedTime
                                        , measurementResult.getDownloadStartTime(), measurementResult.getDownloadStopTime()
                                        , measurementResult.getDownloadElapsedTime(),elapsedTime + measurementResult.getDownloadElapsedTime()
                                );
                                synchronized (logWriter) {
                                    logWriter.write(logEntry);
                                }
                            }
                        }else{

                            reqFileName = String.format(targetFile3, finalI);
                            measurementResult = performSocketQuery(finalServerAddress,finalServerPort,reqFileName,type,downloadPath);
                            long startTime = measurementResult.getCallLookUpStartTime();
                            long endTime = measurementResult.getCallLookUpStopTime();
                            long elapsedTime = measurementResult.getCallLookUpElapsedTime();
                            logEntry = String.format(
                                    "%s,%d,%d,%d,%d,%d,%d,%d%n",
                                    reqFileName, startTime, endTime, elapsedTime
                                    , measurementResult.getDownloadStartTime(), measurementResult.getDownloadStopTime()
                                    , measurementResult.getDownloadElapsedTime(),elapsedTime + measurementResult.getDownloadElapsedTime()
                            );
                            synchronized (logWriter) {
                                logWriter.write(logEntry);
                            }
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });

                start = end + 1;
            }

            threadPool.shutdown();
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            logWriter.flush();
            logWriter.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private MeasurementResult performSocketQuery(String serverAddress,String serverPort,String targetFile,int type,String downloadPath)  {
        Socket peerClientSocket =null;
        ObjectOutputStream out=null;
        ObjectInputStream in=null;
        MeasurementResult measurementResult = new MeasurementResult();
        measurementResult.setCallLookUpStartTime(System.currentTimeMillis());
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
            indexRequest.getIndexSearch().setFileName(targetFile);

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
                        if(measurementResult.getLookupItem()==null){

                            measurementResult.setLookupItem(lookupItem);
                        }
                    }
                } else {
                    System.err.println("File retrieval failed, failure message: The host list is empty");
                }
            }

            measurementResult.setCallLookUpStopTime(System.currentTimeMillis());

            measurementResult.setCallLookUpElapsedTime(measurementResult.getCallLookUpStopTime()-measurementResult.getCallLookUpStartTime());
            indexRequest = new IndexRequest();
            indexRequest.setRequestType(RequestTypeEnum.DISCONNECT.getCode());
            out.writeObject(indexRequest);
            System.out.println("peer client lookup exit complete.");

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


        //Download file
        if(type!=0){
            if(measurementResult.getLookupItem()==null){
                throw new RuntimeException("File not found,fileName:"+targetFile);
            }
            this.downloadFile(measurementResult,downloadPath);
        }

        return measurementResult;

    }

    private void downloadFile(MeasurementResult measurementResult,String downloadPath){

        String fileHostAddress = measurementResult.getLookupItem().getFileServerAddress();
        int fileHostPort = measurementResult.getLookupItem().getFileServerPort();
        String serverFilePath = measurementResult.getLookupItem().getFileLocalPath();
        String serverFileName = measurementResult.getLookupItem().getFileLocalFileName();

        FileReceiver fileReceiver = new FileReceiver();
        lock.lock();
        try{
            System.out.println("Thread "+Thread.currentThread().getName() +" start p2p and obtain file "+serverFileName);
            measurementResult.setDownloadStartTime(System.currentTimeMillis());
            fileReceiver.receiveFile(serverFilePath+serverFileName,serverFileName,fileHostAddress,fileHostPort,downloadPath);
            measurementResult.setDownloadStopTime(System.currentTimeMillis());
            System.out.println("Thread "+Thread.currentThread().getName() +" Obtain the "+serverFileName+" file. The operation is complete ");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            lock.unlock();
        }
        measurementResult.setDownloadElapsedTime(measurementResult.getDownloadStopTime()-measurementResult.getDownloadStartTime());

    }




}
