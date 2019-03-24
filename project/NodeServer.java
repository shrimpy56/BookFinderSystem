import org.apache.thrift.TException;
import org.apache.thrift.server.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.net.InetAddress;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.math.*;

public class NodeServer {

    private static long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());

            return new BigInteger(md.digest()).intValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String [] args) {
        try {
            // pass master server ip, server port, node port.
            String serverIP = args[0];
            int serverPort = Integer.parseInt(args[1]);
            int port = Integer.parseInt(args[2]);

            // register node
            TTransport transport = new TSocket(serverIP, serverPort);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SuperNode.Client serverClient = new SuperNode.Client(protocol);
            transport.open();
            NodeInfo nodeInfo = serverClient.join(InetAddress.getLocalHost().getHostAddress(), port);
            transport.close();

            if (nodeInfo.nack)
            {
                System.out.println("SuperNode is busy in join process of another node! Join failed!");
                return;
            }

            long myNodeId = nodeInfo.newNodeId;

            //todo

            //todo
            System.out.println("join finish.");

            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(port);
            TTransportFactory factory = new TFramedTransport.Factory();

            //Create service request handler
            NodeHandler handler = new NodeHandler();
            handler.setData(serverIP, serverPort, myNodeId);
            Node.Processor processor = new Node.Processor(handler);

            //Set server arguments
            TThreadPoolServer.Args arguments = new TThreadPoolServer.Args(serverTransport);
            arguments.processor(processor);  //Set handler
            arguments.transportFactory(factory);  //Set FramedTransport (for performance)

            //todo: void postJoin(1: string ip, 2: i32 port),

            System.out.println("Node running on: " + InetAddress.getLocalHost().getHostAddress() + ":" + port);

            //Run server
            TServer server = new TThreadPoolServer(arguments);
            server.serve();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }
}

