import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.io.*;
import java.util.*;
import java.lang.*;

public class Client {
    public static void main(String [] args) {
        //Create client connect.
        try {
            // pass params in
            String serverIP = args[0];
            int serverPort = Integer.parseInt(args[1]);

            TTransport transport = new TSocket(serverIP, serverPort);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SuperNode.Client superClient = new SuperNode.Client(protocol);
            //Try to connect
            transport.open();
            NodeInfo node = superClient.getNode();
            transport.close();

            System.out.println("Usage: set <title> <genre> [-withlogs]");
            System.out.println("Usage: set <filename> [-withlogs]");
            System.out.println("Usage: get <title> [-withlogs]");

            transport = new TSocket(node.ip, node.port);
            protocol = new TBinaryProtocol(new TFramedTransport(transport));
            Node.Client client = new Node.Client(protocol);

            Scanner input = new Scanner(System.in);
            while (true)
            {
                String line = input.nextLine();
                String[] params = line.split(" ");
                if (params.length < 2) continue;

                boolean withlog = params[params.length - 1].equals("-withlogs");
                if (params[0].equals("set"))
                {
                    if (withlog && params.length == 3 || !withlog && params.length == 2) //filename
                    {
                        File file = new File(params[1]);
                        Scanner fileInput = new Scanner(file);
                        while (fileInput.hasNext()) {
                            String newLine = fileInput.nextLine();
                            String[] data = newLine.split(":");

                            transport.open();
                            client.setGenre(data[0], data[1], withlog);
                            transport.close();

                            System.out.println("set: " + data[0] + " : " + data[1]);
                        }
                        fileInput.close();
                    }
                    else //title genre
                    {
                        System.out.println("seting: " + params[1] + " : " + params[2]);

                        transport.open();
                        client.setGenre(params[1], params[2], withlog);
                        transport.close();

                        System.out.println("set: " + params[1] + " : " + params[2]);
                    }
                }
                else if (params[0].equals("get"))
                {
                    transport.open();
                    String genre = client.getGenre(params[1], withlog);
                    transport.close();
                    System.out.println("===============================================");
                    System.out.println("Genre of '" + params[1] + "' is " + genre);
                    System.out.println("===============================================");
                }
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
