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

public class IndexServer {

    private final ConcurrentMap<String,FilesStoreEntity> indexFilesStore = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<String>> searchFilesMapping = new ConcurrentHashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock rLock = rwLock.readLock();
    private final Lock wLock = rwLock.writeLock();
    private static final int DEFAULT_SERVER_PORT=8080;
    private static final int FILE_SERVER_DEFAULT_PORT=10000;
    private static final String PEER_KEY_FORMAT = "peer_key_Id:%s_ip:%s";
    private static final int THREAD_POOL_SIZE = 1024;
    public static void main(String[] args) throws IOException {
        new IndexServer().startServer(DEFAULT_SERVER_PORT);
    }

    public void startServer(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        ExecutorService workerThreadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        System.out.println("The index service is started successfully. port: "+DEFAULT_SERVER_PORT);
        while (true) {
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

                        System.out.println("The client has requested that the connection be closed");
                        return ;
                    default:
                        this.writeFailedResult("Unrecognized request type,requestType:"+peerRequest.getRequestType(),outputStream);
                        break;
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("server handleClient error,msg:"+e.getMessage());
            e.printStackTrace();
        }finally {
            if(clientSocket!=null && clientSocket.isConnected()){
                try{
                    clientSocket.close();
                }catch (IOException e){
                    System.out.println("Couldn't close a socket.");
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
            System.out.println(message);
            out.writeObject(IndexResponse.failedResp(message));
        }
    }

    private IndexResponse register(IndexRequest.IndexRegister indexRegister, String peerAddress) {
        System.out.println("Register path: "+indexRegister.getFilePath()+",files.size: "+ indexRegister.getFiles().size()+" from Peer(" + peerAddress+" ), peerId:"+indexRegister.getPeerId());

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
                filesStoreEntity.setFileServerPort(FILE_SERVER_DEFAULT_PORT);
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
                filesStoreEntity.setFileServerPort(FILE_SERVER_DEFAULT_PORT);
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
            System.out.println("save files to indexFilesStore error,error:"+e.getMessage());
            return IndexResponse.failedResp("save files to indexFilesStore error,error:"+e.getMessage());
        }finally {
            wLock.unlock();
        }

        System.out.println("The peer (ID:"+indexRegister.getPeerId()+", IP:"+peerAddress+") sent "+indexRegister.getFiles().size()+" files to the server. ");

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
            System.out.println("indexFilesStore remove peerKey:"+ peerKey);
            if(filesStoreEntity!=null){
                for(Map.Entry<String,List<String>> entry: filesStoreEntity.getPathFilesMapping().entrySet()){
                    fileList = entry.getValue();
                    if(fileList!=null && fileList.size()>0){
                        for(String file:fileList){
                            searchFilesMapping.remove(file);
                            System.out.println("searchFilesMapping remove item. path: "+entry.getKey()+" ,fileKey: "+ file);
                        }
                    }
                }

            }
        }catch (Exception e){
            System.err.println("save files to indexFilesStore error,error:"+e.getMessage());
            return IndexResponse.failedResp("save files to indexFilesStore error,error:"+e.getMessage());
        }finally {
            wLock.unlock();
        }
        IndexResponse.ResultData resultData = new IndexResponse.ResultData();
        resultData.setPeerId(peerId);
        return IndexResponse.sucResp(resultData);
    }
    private IndexResponse lookup(String fileName) {
        System.out.println("lookup file "+fileName);

        rLock.lock();
        try{
            Set<String> peerKeySet = this.searchFilesMapping.get(fileName);
            if(peerKeySet==null || peerKeySet.size()<1){
                System.err.println("No file found, name:"+fileName);
                return IndexResponse.failedResp("No file found, name:"+fileName);
            }
            IndexResponse.ResultData resultData = new IndexResponse.ResultData();
            resultData.setPeerAndIpMapping(new HashMap<>());
            int num=1;
            String fileLocalPath = null;
            for(String peerKey:peerKeySet){
                FilesStoreEntity filesStoreEntity = this.indexFilesStore.get(peerKey);
                if(filesStoreEntity==null){
                    System.err.println("Not found filesStoreEntity, name:"+fileName+" ,peerKey:"+peerKey);
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
            System.out.println("call lookup error,error:"+e.getMessage());
            return IndexResponse.failedResp("call lookup error,error:"+e.getMessage());
        }finally {
            rLock.unlock();
        }
    }
}