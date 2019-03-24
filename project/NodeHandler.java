import org.apache.thrift.TException;
import org.apache.thrift.server.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;

import java.io.*;
import java.util.*;
import java.lang.*;

public class NodeHandler implements Node.Iface
{
    private long nodeID;
    private String serverIP;
    private int serverPort;

    void setData(String serverIP, int serverPort, long nodeID)
    {
        this.serverIP = serverIP;
        this.serverPort = serverPort;
        this.nodeID = nodeID;
    }

    @Override
    public void setGenre(String title, String genre, boolean withLogs) throws org.apache.thrift.TException
    {
        //
    }

    @Override
    public String getGenre(String title, boolean withLogs) throws org.apache.thrift.TException
    {
        //
    }

    @Override
    public void updateDHT() throws org.apache.thrift.TException
    {

    }

    @Override
    public void printDHT() throws org.apache.thrift.TException
    {
        //Node ID - range of keys - predecessor - successor - number of cities stored Cities list
        //Finger Table
    }
}

