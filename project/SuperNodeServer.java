import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.net.InetAddress;

public class SuperNodeServer {
    public static SuperNodeHandler handler;
    public static SuperNode.Processor processor;

    public static void main(String [] args) {
        try {
            //pass params in
            int port = Integer.parseInt(args[0]);
            //number of nodes
            long numOfNodes = Math.pow(2, 16) - 1;//default value
            if (args.length > 1)
            {
                numOfNodes = Long.parseLong(args[1]);
            }

            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(port);
            TTransportFactory factory = new TFramedTransport.Factory();

            //Create service request handler
            handler = new SuperNodeHandler();
            handler.setData(port, numOfNodes);
            processor = new SuperNode.Processor(handler);

            //Set server arguments
            TThreadPoolServer.Args arguments = new TThreadPoolServer.Args(serverTransport);
            arguments.processor(processor);  //Set handler
            arguments.transportFactory(factory);  //Set FramedTransport (for performance)

            System.out.println("SuperNode running on: " + InetAddress.getLocalHost().getHostAddress() + ":" + port);

            //Run server as a single thread
            TServer server = new TThreadPoolServer(arguments);
            server.serve();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }
}

