import org.apache.thrift.TException;
import org.apache.thrift.server.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.io.*;
import java.util.*;
import java.lang.*;

public class SuperNodeHandler implements SuperNode.Iface
{
    private List<NodeInfo> nodeList = new ArrayList<>();
    private HashSet<Long> idSet = new HashSet<>();
    private int port;
    private boolean bIsJoining = false;
    private long numOfNodes;

    void setData(int port, long numOfNodes)
    {
        this.port = port;
        this.numOfNodes = numOfNodes;
    }

    private static long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());

            return new BigInteger(md.digest()).intValue() % numOfNodes;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public NodeInfo join(String ip, int port) throws org.apache.thrift.TException
    {
        NodeInfo info = new NodeInfo();
        if (bIsJoining)
        {
            info.nack = true;
            return info;
        }
        bIsJoining = true;

        info.nack = false;
        info.ip = ip;
        info.port = port;
        info.maxNodeNum = numOfNodes;
        long newId = hash(ip+port);
        while (idSet.contains(newId))
            newId = (newId + 1) % numOfNodes;
        idSet.add(newId);
        info.nodeId = newId;
        nodeList.add(info);

        NodeInfo randNode = getNode();
        randNode.newNodeId = info.nodeId;
        return randNode;
    }

    @Override
    public void postJoin(String ip, int port) throws org.apache.thrift.TException
    {
        bIsJoining = false;
    }

    @Override
    public NodeInfo getNode() throws org.apache.thrift.TException
    {
        if (nodeList.size() == 0)
        {
            return null;
        }
        int idx = (int) (Math.random() * nodeList.size());
        return nodeList.get(idx);
    }
}

