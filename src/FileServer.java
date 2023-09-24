import java.io.*;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FileServer {

    public static final int FILE_SERVER_DEFAULT_PORT=10000;

    private static final int INPUT_BUFFER_SIZE = 4096;

    private final DatagramSocket commandSocket;
    private final ExecutorService workerThreadPool;
    private static final int THREAD_POOL_SIZE = 1024;
    public FileServer(int port) throws SocketException {
        this.commandSocket = new DatagramSocket(port);
        this.workerThreadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        System.out.println("\nThe file server is started successfully. port: " + port);
    }

    public void startFileServer() {
        while(true) {
            try {
                byte[] inputDataBuffer = new byte[INPUT_BUFFER_SIZE];
                DatagramPacket inputPacket = new DatagramPacket(inputDataBuffer, inputDataBuffer.length);
                commandSocket.receive(inputPacket);
                // The client connection is handled by the worker thread pool
                workerThreadPool.submit(() -> accept(inputPacket));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void accept(DatagramPacket inputPacket) {
        String command = new String(inputPacket.getData());
        String ipAddress = inputPacket.getAddress().toString().substring(1);
        int port = inputPacket.getPort();
        byte[] outputDataBuffer;

        try {
            if (command.startsWith("GET ")) {
                String[] commands = command.split(" ");
                int sendStreamPort = Integer.parseInt(commands[1]);
                String fileName = commands[2];
                long fileSize = FileUtils.getFileSizeInBytes(fileName);
                if (fileSize>0) {
                    outputDataBuffer = ("ACCEPT "+fileSize+" ").getBytes();

                    sendDatagram(outputDataBuffer, ipAddress, port);
                    TimeUnit.MILLISECONDS.sleep(500);
                    sendFileStream(ipAddress, sendStreamPort, fileName);
                } else {
                    outputDataBuffer = "FAILED".getBytes();
                    sendDatagram(outputDataBuffer, ipAddress, port);
                }
            }
        } catch (Exception ex) {
            System.err.println("accept error, message: " + ex.getMessage());
        }
    }


    private void sendDatagram(byte[] outputBuffer, String ipAddress, int port) {
        try (DatagramSocket clientSocket = new DatagramSocket()) {
            InetAddress inetAddress = InetAddress.getByName(ipAddress);
            DatagramPacket outputPacket = new DatagramPacket(outputBuffer, outputBuffer.length, inetAddress, port);
            clientSocket.send(outputPacket);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private void sendFileStream(String ipAddress, int port, String fileName) {
        Socket clientFileSocket = null;
        DataInputStream fileInputStream = null;
        DataOutputStream fileOutputStream = null;

        try {
            clientFileSocket = new Socket(ipAddress, port);
            fileInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(fileName)));
            fileOutputStream = new DataOutputStream(clientFileSocket.getOutputStream());

            byte[] fileBuffer = new byte[INPUT_BUFFER_SIZE];

            int bytesRead;

            while ((bytesRead = fileInputStream.read(fileBuffer)) != -1) {
                fileOutputStream.write(fileBuffer, 0, bytesRead);
            }
            fileOutputStream.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            // Close Socket and DataStream
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
                if (clientFileSocket != null) {
                    clientFileSocket.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }


}
