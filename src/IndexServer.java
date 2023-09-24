import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class IndexServer {

    private static Logger LOGGER = Logger.getLogger(IndexServer.class.getName());
    private final ConcurrentMap<String,FilesStoreEntity> indexFilesStore = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> searchFilesMapping = new ConcurrentHashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();

    private static final String PEER_KEY_FORMAT = "peer_key_Id:%s_ip:%s";

    private static void configureLogging() {
        try {
            // Set the log output format
            System.setProperty("java.util.logging.SimpleFormatter.format",
                    "[%1$tF %1$tT] [%4$-7s] %5$s %n");

            // Create a log handler and output logs to a specified file
            FileHandler fileHandler = new FileHandler("index_server.log");

            // Set the output format of the log processor
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);

            // Obtain the root Logger and add a file handler
            Logger rootLogger = Logger.getLogger("");
            rootLogger.addHandler(fileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        configureLogging();
        new IndexServer().startServer(ConstantUtils.INDEX_SERVER_PORT);
    }

    public void startServer(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        ExecutorService workerThreadPool = Executors.newFixedThreadPool(ConstantUtils.THREAD_POOL_SIZE);

        LOGGER.info("The index service is started successfully. port: "+ConstantUtils.INDEX_SERVER_PORT);
        while(true) {
            try {
                Socket clientSocket = serverSocket.accept();
                // 将客户端连接交给工作线程池处理
                workerThreadPool.submit(() -> handleClient(clientSocket));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleClient(Socket clientSocket)  {
        try {

            ObjectInputStream inputStream = new ObjectInputStream(clientSocket.getInputStream());
            ObjectOutputStream outputStream = new ObjectOutputStream(clientSocket.getOutputStream());

            String clientIp = clientSocket.getInetAddress().getHostAddress();

            this.writeSucResult(IndexResponse.sucResp("Connection established successfully"),outputStream);

            while(true){
                IndexRequest peerRequest = (IndexRequest) inputStream.readObject();

                if(peerRequest==null){
                    this.writeResult(false,null,"The request object is empty",outputStream);
                    return ;
                }

                IndexResponse indexResponse;
                switch (RequestTypeEnum.getEnumByCode(peerRequest.getRequestType())){
                    case REGISTER:
                        IndexRequest.IndexRegister indexRegister =  peerRequest.getIndexRegister();
                        if(indexRegister==null || indexRegister.getPeerId()==null
                                || "".equals(indexRegister.getPeerId()) || indexRegister.getFiles()==null
                                || indexRegister.getFiles().size()<1 || indexRegister.getFilePath()==null
                                || "".equals(indexRegister.getFilePath())){

                            this.writeFailedResult("The request IndexRegister is invalid",outputStream);
                            return ;
                        }

                        indexResponse = register( indexRegister ,clientIp);
                        this.writeSucResult(indexResponse,outputStream);

                        break;
                    case UNREGISTER:
                        IndexRequest.IndexRegister unRegister =  peerRequest.getIndexRegister();
                        if(unRegister==null || unRegister.getPeerId()==null || "".equals(unRegister.getPeerId())  ){
                            this.writeFailedResult("The request IndexRegister is invalid",outputStream);
                            return ;
                        }

                        indexResponse = unRegister(unRegister.getPeerId(),clientIp);

                        this.writeSucResult(indexResponse,outputStream);
                        break;
                    case LOOKUP:
                        IndexRequest.IndexSearch indexSearch =  peerRequest.getIndexSearch();
                        if(indexSearch==null || indexSearch.getFileName()==null){
                            this.writeFailedResult("The request IndexSearch is invalid",outputStream);
                            return ;
                        }
                        indexResponse = lookup(indexSearch.getFileName());

                        this.writeSucResult(indexResponse,outputStream);
                        break;
                    case DISCONNECT:

                        LOGGER.info("The client has requested that the connection be closed");
                        return ;
                    default:
                        this.writeFailedResult("Unrecognized request type,requestType:"+peerRequest.getRequestType(),outputStream);
                        break;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.info("server handleClient error,msg:"+e.getMessage());
            e.printStackTrace();
        }finally {
            if(clientSocket!=null && clientSocket.isConnected()){
                try{
                    clientSocket.close();
                }catch (IOException e){
                    LOGGER.info("Couldn't close a socket.");
                }

            }
        }
    }

    private void writeSucResult(IndexResponse indexResponse,ObjectOutputStream out) throws IOException {
        this.writeResult(true,indexResponse,null,out);
    }
    private void writeFailedResult(String message,ObjectOutputStream out) throws IOException {
        this.writeResult(false,null,message,out);
    }
    private void writeResult(boolean suc,IndexResponse indexResponse,String message,ObjectOutputStream out) throws IOException {
        if(suc){
            out.writeObject(indexResponse);
        }else{
            LOGGER.info(message);
            out.writeObject(IndexResponse.failedResp(message));
        }
    }

    private IndexResponse register(IndexRequest.IndexRegister indexRegister, String peerAddress) {
        LOGGER.info("Register path: "+indexRegister.getFilePath()+",files.size: "+ indexRegister.getFiles().size()+" from Peer(" + peerAddress+" ), peerId:"+indexRegister.getPeerId());

        String peerKeyStr = String.format(PEER_KEY_FORMAT,indexRegister.getPeerId(), peerAddress);
        List<String> fileList;
        FilesStoreEntity filesStoreEntity;
        Date date = new Date();
        Set<String> peerKeyList;
        wLock.lock();
        try{
            if(indexFilesStore.containsKey(peerKeyStr)){
                filesStoreEntity = indexFilesStore.get(peerKeyStr);
                filesStoreEntity.setAddTime(date);
                filesStoreEntity.setFileServerAddress(peerAddress);
                filesStoreEntity.setFileServerPort(ConstantUtils.FILE_SERVER_DEFAULT_PORT);
                if(filesStoreEntity.getPathFilesMapping().containsKey(indexRegister.getFilePath())){
                    fileList = filesStoreEntity.getPathFilesMapping().get(indexRegister.getFilePath());
                    List<String> needAddList = new ArrayList<>();
                    boolean addFlag = true;
                    for(String addFileName:indexRegister.getFiles()){
                        for(String fileName:fileList){
                            if(fileName.equals(addFileName)){
                                addFlag = false;
                                break;
                            }
                        }
                        if(addFlag){
                            needAddList.add(addFileName);
                        }

                        addFlag = true;
                    }
                    fileList.addAll(needAddList);
                }else{
                    fileList = new ArrayList<>(indexRegister.getFiles());
                    filesStoreEntity.getPathFilesMapping().put(indexRegister.getFilePath(),fileList);
                }


            }else{
                fileList = new ArrayList<>(indexRegister.getFiles());
                filesStoreEntity = new FilesStoreEntity();
                filesStoreEntity.setAddTime(date);
                filesStoreEntity.setFileServerAddress(peerAddress);
                filesStoreEntity.setFileServerPort(ConstantUtils.FILE_SERVER_DEFAULT_PORT);
                filesStoreEntity.setPeerId(indexRegister.getPeerId());
                filesStoreEntity.setPathFilesMapping(new HashMap<>());
                filesStoreEntity.getPathFilesMapping().put(indexRegister.getFilePath(),fileList);
                indexFilesStore.put(peerKeyStr,filesStoreEntity);
            }

            if(fileList.size() > 0){
                for(String file:fileList){
                    if(searchFilesMapping.containsKey(file)){
                        peerKeyList = searchFilesMapping.get(file);
                        peerKeyList.add(peerKeyStr);
                    }else{
                        peerKeyList = new HashSet<>();
                        peerKeyList.add(peerKeyStr);
                        searchFilesMapping.put(file,peerKeyList);
                    }
                }

            }

        }catch (Exception e){
            LOGGER.info("save files to indexFilesStore error,error:"+e.getMessage());
            return IndexResponse.failedResp("save files to indexFilesStore error,error:"+e.getMessage());
        }finally {
            wLock.unlock();
        }

        LOGGER.info("The peer (ID:"+indexRegister.getPeerId()+", IP:"+peerAddress+") sent "+indexRegister.getFiles().size()+" files to the server. ");

        IndexResponse.ResultData resultData = new IndexResponse.ResultData();
        resultData.setPeerId(indexRegister.getPeerId());
        resultData.setFiles((ArrayList<String>) fileList);
        return IndexResponse.sucResp(resultData);

    }

    private IndexResponse unRegister(String peerId, String peerAddress) {

        String peerKey = String.format(PEER_KEY_FORMAT,peerId, peerAddress);
        List<String> fileList;
        wLock.lock();
        try{
            FilesStoreEntity filesStoreEntity = indexFilesStore.remove(peerKey);
            LOGGER.info("indexFilesStore remove peerKey:"+ peerKey);
            if(filesStoreEntity!=null){
                for(Map.Entry<String,List<String>> entry: filesStoreEntity.getPathFilesMapping().entrySet()){
                    fileList = entry.getValue();
                    if(fileList!=null && fileList.size()>0){
                        for(String file:fileList){
                            searchFilesMapping.remove(file);
                            LOGGER.info("searchFilesMapping remove item. path: "+entry.getKey()+" ,fileKey: "+ file);
                        }
                    }
                }

            }
        }catch (Exception e){
            LOGGER.severe("save files to indexFilesStore error,error:"+e.getMessage());
            return IndexResponse.failedResp("save files to indexFilesStore error,error:"+e.getMessage());
        }finally {
            wLock.unlock();
        }
        IndexResponse.ResultData resultData = new IndexResponse.ResultData();
        resultData.setPeerId(peerId);
        return IndexResponse.sucResp(resultData);
    }
    private IndexResponse lookup(String fileName) {
        LOGGER.info("lookup file "+fileName);

        rLock.lock();
        try{
            Set<String> peerKeySet = this.searchFilesMapping.get(fileName);
            if(peerKeySet==null || peerKeySet.size()<1){
                LOGGER.severe("No file found, name:"+fileName);
                return IndexResponse.failedResp("No file found, name:"+fileName);
            }
            IndexResponse.ResultData resultData = new IndexResponse.ResultData();
            resultData.setPeerAndIpMapping(new HashMap<>());
            int num=1;
            String fileLocalPath = null;
            for(String peerKey:peerKeySet){
                FilesStoreEntity filesStoreEntity = this.indexFilesStore.get(peerKey);
                if(filesStoreEntity==null){
                    LOGGER.severe("Not found filesStoreEntity, name:"+fileName+" ,peerKey:"+peerKey);
                    continue;
                }

                for(Map.Entry<String,List<String>> entry:filesStoreEntity.getPathFilesMapping().entrySet()){
                    for(String value : entry.getValue()){
                        if(value.equals(fileName)){
                            fileLocalPath = entry.getKey();
                            break;
                        }
                    }
                    if(fileLocalPath!=null){
                        break;
                    }
                }

                IndexResponse.LookupItem lookupItem = new IndexResponse.LookupItem();
                lookupItem.setPeerId(filesStoreEntity.getPeerId());
                lookupItem.setFileServerAddress(filesStoreEntity.getFileServerAddress());
                lookupItem.setFileServerPort(filesStoreEntity.getFileServerPort());
                lookupItem.setFileLocalPath(fileLocalPath);
                lookupItem.setFileLocalFileName(fileName);
                resultData.getPeerAndIpMapping().put(num,lookupItem);
                num++;
            }

            return IndexResponse.sucResp(resultData);
        }catch (Exception e){
            LOGGER.severe("call lookup error,error:"+e.getMessage());
            return IndexResponse.failedResp("call lookup error,error:"+e.getMessage());
        }finally {
            rLock.unlock();
        }
    }
}
