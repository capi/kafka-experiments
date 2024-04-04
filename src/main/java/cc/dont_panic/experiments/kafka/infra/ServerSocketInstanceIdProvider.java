package cc.dont_panic.experiments.kafka.infra;

import java.io.IOException;
import java.net.ServerSocket;

public class ServerSocketInstanceIdProvider implements InstanceIdProvider, AutoCloseable {

    private final int minPortNumber;
    private final int maxPortNumber;
    private int portNumber;
    private ServerSocket serverSocket;

    public ServerSocketInstanceIdProvider(int minPortNumber, int maxPortNumber) {
        this.minPortNumber = minPortNumber;
        this.maxPortNumber = maxPortNumber;
    }

    @Override
    public int getInstanceId() {
        if (portNumber > 0) {
            return portNumber;
        }
        for (int port = minPortNumber; port <= maxPortNumber; port++) {
            try {
                serverSocket = new ServerSocket(port);
                portNumber = port;
                return port;
            } catch (IOException e) {
                // ignored, up to the next
            }
        }
        throw new RuntimeException("No port available in range %d to %d".formatted(minPortNumber, maxPortNumber));
    }

    @Override
    public void close() throws IOException {
        if (serverSocket != null) {
            serverSocket.close();
            serverSocket = null;
        }
    }
}
