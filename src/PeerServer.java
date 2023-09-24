import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class PeerServer {

//    private static Logger LOGGER = Logger.getLogger("peer_client");
    private static void configureLogging() {
        try {
            // Set the log output format
            System.setProperty("java.util.logging.SimpleFormatter.format",
                    "[%1$tF %1$tT] [%4$-7s] %5$s %n");

            // Create a log handler and output logs to a specified file
            FileHandler fileHandler = new FileHandler("peer_server.log");

            // Set the output format of the log processor
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);

            // Obtain the root Logger and add a file handler
            Logger rootLogger = Logger.getLogger("peer_server");
            rootLogger.addHandler(fileHandler);


//            FileHandler clientFileHandler = new FileHandler("peer_client.log");
//            clientFileHandler.setFormatter(formatter);
//            Logger clientLogger = Logger.getLogger("peer_client");
//            clientLogger.addHandler(clientFileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        configureLogging();
        String peerId = UUID.randomUUID().toString();
        Thread peerClientThread = new Thread(new PeerClient(peerId));
        peerClientThread.start();

        //File Server
        FileServer fileServer = new FileServer(ConstantUtils.FILE_SERVER_DEFAULT_PORT);
        fileServer.startFileServer();

    }

    public static class PeerClient implements Runnable{
        private final String peerId;
        public PeerClient(String peerId){
            this.peerId=peerId;
        }
        @Override
        public void run() {
            Socket peerClientSocket = null;
            BufferedReader input = null;
            ObjectInputStream in = null;
            ObjectOutputStream out = null;
            IndexRequest indexRequest;
            IndexResponse indexServerResponse;

            try {
                TimeUnit.SECONDS.sleep(1);
                System.out.println("peer initiates the registration of the server. peerId:"+peerId);
                input = new BufferedReader(new InputStreamReader(System.in));
                System.out.print("Enter Server IP Address (The default address is 127.0.0.1):");
                String serverAddress = input.readLine();

                if(serverAddress.trim().length() == 0 || "\n".equals(serverAddress)) {
                    serverAddress = ConstantUtils.DEFAULT_INDEX_SERVER_HOST;
                }

                if(!IPAddressValidator.isValidIP(serverAddress)) {
                    System.out.println("Invalid Server IP Address.");
                    System.exit(0);
                }

                System.out.print("Enter Server PORT (The default PORT is 9000):");
                String serverPort = input.readLine();
                if(serverPort.trim().length() == 0 || "\n".equals(serverPort)) {
                    serverPort = String.valueOf(ConstantUtils.INDEX_SERVER_PORT);
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

                while (true) {
                    //Show the user different choices
                    System.out.println("\nPlease follow the prompts below to make your selection and enter the number!");
                    System.out.println("1.Register all its files with the indexing serve.");
                    System.out.println("2.Search the index and return all the matching.");
                    System.out.println("3.Unregister all files for this peer from the index server.");
                    System.out.println("4.Exit.");
                    System.out.print("Only accepts numeric input:");
                    int option;
                    try {
                        option = Integer.parseInt(input.readLine());
                    } catch (NumberFormatException e) {
                        System.out.println("Wrong choice. Try again!!!");
                        continue;
                    }

                    switch (option) {
                        case 1:
                            System.out.println("\nPlease fill in the destination folder you want to sync:");
                            String filePath = input.readLine();

                            if(filePath.trim().length() == 0) {
                                System.out.println("Invalid path, please try again.");
                                continue;
                            }

                            FileUtils.DirectoryEntity directoryEntity = FileUtils.listFilesInDirectoryOrFile(filePath);

                            if (directoryEntity!=null && directoryEntity.getFileNames()!=null && directoryEntity.getFileNames().size()>0) {

                                indexRequest = new IndexRequest();
                                indexRequest.setRequestType(RequestTypeEnum.REGISTER.getCode());
                                indexRequest.setIndexRegister(new IndexRequest.IndexRegister());
                                indexRequest.getIndexRegister().setPeerId(this.peerId);
                                indexRequest.getIndexRegister().setFiles((ArrayList<String>) directoryEntity.getFileNames());
                                indexRequest.getIndexRegister().setFilePath(directoryEntity.getFileDirectory());
                                out.writeObject(indexRequest);

                                indexServerResponse = (IndexResponse) in.readObject();

                                if (indexServerResponse.isSuc()) {
                                    System.out.println(directoryEntity.getFileNames().size() + " files registered with indexing server. ");
                                } else {
                                    System.err.println("Unable to register files with server. Please try again later.");
                                }
                            } else {
                                System.out.println("0 files found at this location. Nothing registered with indexing server.");
                            }
                            break;

                        // Query files from the indexing service
                        case 2:
                            System.out.println("\nEnter the name of the file you want to find on the index server:");
                            String fileName = input.readLine();
                            String fileHostAddress;
                            int fileHostPort;
                            String serverFilePath;
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
                                        lookupItem =entry.getValue();
                                        System.out.println("\nNumber: "+entry.getKey()+" , Peer ID:" + lookupItem.getPeerId() + ", Host Address:" + lookupItem.getFileServerAddress()+":"+lookupItem.getFileServerPort());
                                        if(firstItem==null){
                                            firstItem = entry.getValue();
                                        }
                                    }
                                }else{
                                    System.err.println("File retrieval failed, failure message: The host list is empty" );
                                    break;
                                }

                                // If a file is text, provide two options to download or print
                                if (fileName.trim().endsWith(".txt")) {
                                    System.out.print("\nDo you want to download (D) or print this file (P)? Enter (D/P):");
                                    String download = input.readLine();

                                    // If multiple peer services are available, the user needs to select one of them
                                    if(lookupMap.size() > 1) {
                                        System.out.print("\nEnter Number from which you want to download the file:");
                                        int number = Integer.parseInt(input.readLine());
                                        firstItem = lookupMap.get(number);
                                    }

                                    if(firstItem==null){
                                        System.err.println("The number entered is incorrect.");
                                        break;
                                    }

                                    fileHostAddress = firstItem.getFileServerAddress();
                                    fileHostPort = firstItem.getFileServerPort();
                                    serverFilePath = firstItem.getFileLocalPath();

                                    if(serverFilePath==null){
                                        System.err.print("The path of the destination file remote server is incorrect.");
                                        break;
                                    }

                                    if (download.equalsIgnoreCase("D")) {
                                        System.out.println("The download file you select will be downloaded to the 'downloads' folder.");
                                        downloadFileLocation=downloadFile(fileHostAddress, fileHostPort, serverFilePath+fileName,fileName);
                                    } else if (download.equalsIgnoreCase("P")) {
                                        downloadFileLocation =downloadFile(fileHostAddress, fileHostPort, serverFilePath+fileName,fileName);
                                        FileUtils.readAndOutputFile(downloadFileLocation);
                                    }
                                } else {
                                    System.out.print("\nDo you want to download this file?(Y/N):");
                                    String download = input.readLine();
                                    if (download.equalsIgnoreCase("Y")) {
                                        if(lookupMap.size() > 1) {
                                            System.out.print("Enter Number from which you want to download the file:");
                                            int number = Integer.parseInt(input.readLine());
                                            firstItem = lookupMap.get(number);
                                        }
                                        if(firstItem==null){
                                            System.err.print("The number entered is incorrect.");
                                            break;
                                        }

                                        fileHostAddress = firstItem.getFileServerAddress();
                                        fileHostPort = firstItem.getFileServerPort();
                                        serverFilePath = firstItem.getFileLocalPath();

                                        if(serverFilePath==null){
                                            System.err.print("The path of the destination file remote server is incorrect.");
                                            break;
                                        }

                                        downloadFileLocation = downloadFile(fileHostAddress, fileHostPort, serverFilePath+fileName, fileName);
                                    }
                                }
                                System.out.println("All operations completed, download file location: "+downloadFileLocation);
                            } else {
                                System.out.println("File retrieval failed, failure message:" + indexServerResponse.getMessage());
                            }
                            break;

                        //De-registration of the index server
                        case 3:

                            System.out.print("\nAre you sure (Y/N)?:");
                            String confirm = input.readLine();

                            if (confirm.equalsIgnoreCase("Y")) {

                                //Send a de-registration request to the index service
                                indexRequest = new IndexRequest();
                                indexRequest.setRequestType(RequestTypeEnum.UNREGISTER.getCode());
                                indexRequest.setIndexRegister(new IndexRequest.IndexRegister());
                                indexRequest.getIndexRegister().setPeerId(this.peerId);
                                out.writeObject(indexRequest);
                                //Read result
                                indexServerResponse = (IndexResponse) in.readObject();
                                if(indexServerResponse.isSuc()){
                                    System.out.println("unregister successful...");
                                }else{
                                    System.out.println("unregister failure. message: "+indexServerResponse.getMessage());
                                }

                            }
                            break;

                        // Process exit logic
                        case 4:
                            indexRequest = new IndexRequest();
                            indexRequest.setRequestType(RequestTypeEnum.DISCONNECT.getCode());
                            out.writeObject(indexRequest);
                            System.out.println("System exit complete.");
                            return ;
                        default:
                            System.err.println("Incorrect selection, please try again!!!");
                            break;
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
            } finally {
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
                    if (input != null){
                        input.close();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private String downloadFile(String fileHostAddress, Integer fileHostPort,String fileFullName,String fileName) throws Exception {

            FileReceiver fileReceiver = new FileReceiver();
            return fileReceiver.receiveFile(fileFullName,fileName,fileHostAddress,fileHostPort,null);
        }

    }

}
