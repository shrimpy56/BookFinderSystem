import org.apache.thrift.TException;
import org.apache.thrift.server.*;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.math.*;

public class NodeHandler implements Node.Iface
{
    private TabelItem selfItem = new TableItem();
    private String serverIP;
    private int serverPort;
    private List<TableItem> fingerTable = new ArrayList<>();
    private Map<String, String> data;// = new HashMap<>();
    TableItem predecessor;
    private long maxNodeNum;

    private static long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());

            return new BigInteger(md.digest()).intValue() % maxNodeNum;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    boolean init(String serverIP, int serverPort, int port, NodeInfo helperNode)
    {
        this.serverIP = serverIP;
        this.serverPort = serverPort;
        selfItem.nodeId = helperNode.newNodeId;
        this.maxNodeNum = helperNode.maxNodeNum;
        selfItem.ip = InetAddress.getLocalHost().getHostAddress();
        selfItem.port = port;

        fingerTable.clear();
        int m = int(Math.log(maxNodeNum) / Math.log(2));

        TableItem item = new TableItem();
        item.ip = helperNode.ip;
        item.port = helperNode.port;
        item.nodeId = helperNode.nodeId;

        //first node
        if (myNodeId == item.nodeId)
        {
            predecessor = item;

            for(int i = 0; i < m; i++)
            {
                fingerTable.add(item);
            }
        }
        else
        {
            //find succ
            TTransport transport = new TSocket(item.ip, item.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            Node.Client helperClient = new Node.Client(protocol);
            transport.open();
            TableItem succ = helperClient.getSucc((selfItem.nodeId+1) % maxNodeNum);
            fingerTable.add(succ);

            TTransport transport2 = new TSocket(succ.ip, succ.port);
            TProtocol protocol2 = new TBinaryProtocol(new TFramedTransport(transport));
            Node.Client succClient = new Node.Client(protocol2);
            transport2.open();
            predecessor = succClient.getPred();
            succClient.setPred(selfItem);
            transport2.close();

            for(int i = 1; i < m; i++)
            {
                long nodeID = (selfItem.nodeId + Math.pow(2, i)) % maxNodeNum;
                long startnode = selfItem.nodeId;
                long endnode = fingerTable.get(i-1).nodeId;
                if (isIn(nodeID, startnode, endnode) || nodeID == startnode)
                {
                    fingerTable.add(fingerTable.get(i-1));
                }
                else
                {
                    TableItem nodeItem = helperClient.getSucc(nodeID);
                    fingerTable.add(nodeItem);
                }
            }
            transport.close();
        }

        //update
        for(int i = 0; i < m; i++)
        {
            TableItem predecessor = getPredOf((selfItem.nodeId - Math.pow(2, i)) % maxNodeNum);

            TTransport transport = new TSocket(predecessor.ip, predecessor.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            Node.Client predClient = new Node.Client(protocol);
            transport.open();
            predClient.updateDHT(selfItem.nodeId, i);
            transport.close();
        }

        //move key (pred, n]
        TTransport transport2 = new TSocket(succ.ip, succ.port);
        TProtocol protocol2 = new TBinaryProtocol(new TFramedTransport(transport));
        Node.Client succClient = new Node.Client(protocol2);
        transport2.open();
        Map<String, String> newData = succClient.removeDataBeforeNode(selfItem.nodeId);
        transport2.close();
        data = newData;

        //post join
        TTransport transport = new TSocket(serverIP, serverPort);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        SuperNode.Client serverClient = new SuperNode.Client(protocol);
        transport.open();
        serverClient.postJoin(InetAddress.getLocalHost().getHostAddress(), port);
        transport.close();

        return true;
    }

    @Override
    public Map<String, String> removeDataBeforeNode(long nodeID) throws org.apache.thrift.TException
    {
        Map<String, String> ans = new HashMap<>();
        for (Iterator<Map.entry<String, String>> itr = data.entrySet().iterator; itr.hasNext(); )
        {
            Map.Entry<String, String> item = itr.next();
            String key = item.getKey();
            String val = item.getValue();
            long node = hash(key);
            if (isIn(node, predecessor.nodeId, nodeID) || selfItem.nodeId == nodeID)
            {
                ans.put(key, val);
                itr.remove();
            }
        }
        return ans;
    }

    bool isIn(long nodeID, long startnode, long endnode)
    {
        return (endnode > startnode && nodeID < endnode && nodeID > startnode || endnode < startnode && (nodeID < startnode
                && nodeID < endnode || nodeID > startnode && nodeID > endnode));
    }

    @Override
    public void setGenre(String title, String genre, boolean withLogs) throws org.apache.thrift.TException
    {
        long key = hash(title);

        if (isIn(key, predecessor.nodeId, selfItem.nodeId) || key == selfItem.nodeId)
        {
            if (withLogs)
                System.out.println("Setgenre done on node" + selfItem.nodeId);

            data.put(title, genre);
        }
        else
        {
            TableItem finger = getClosestPredFinger(key);

            if (withLogs)
                System.out.println("Forward setgenre to node " + finger.nodeId);

            TTransport transport = new TSocket(finger.ip, finger.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            Node.Client client = new Node.Client(protocol);
            transport.open();
            client.setGenre(title, genre, withLogs);
            transport.close();
        }
    }

    @Override
    public String getGenre(String title, boolean withLogs) throws org.apache.thrift.TException
    {
        long key = hash(title);

        if (isIn(key, predecessor.nodeId, selfItem.nodeId) || key == selfItem.nodeId)
        {
            if (withLogs)
                System.out.println("Getgenre done on node" + selfItem.nodeId);

            return data.get(title);
        }
        else
        {
            TableItem finger = getClosestPredFinger(key);

            if (withLogs)
                System.out.println("Forward getgenre to node " + finger.nodeId);

            TTransport transport = new TSocket(finger.ip, finger.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            Node.Client client = new Node.Client(protocol);
            transport.open();
            String ans = client.getGenre(title, withLogs);
            transport.close();

            return ans;
        }
    }

    @Override
    public void updateDHT(TableItem nodeInfo, int idx) throws org.apache.thrift.TException
    {
        if (isIn(nodeInfo.nodeId, selfItem.nodeId, fingerTable.get(idx).nodeId))
        {
            fingerTable.set(idx, nodeInfo);

            TTransport transport = new TSocket(predecessor.ip, predecessor.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            Node.Client predClient = new Node.Client(protocol);
            transport.open();
            predClient.updateDHT(nodeInfo, idx);
            transport.close();

            //test
            printDHT();
        }
    }

    @Override
    public void printDHT() throws org.apache.thrift.TException
    {
        System.out.println("===============================================");
        System.out.println("Node ID: " + selfItem.nodeId);
        System.out.println("Range of keys: (" + predecessor.nodeId + ", " + selfItem.nodeId + "]");
        System.out.println("Finger Table:");
        for(int i = 0; i < fingerTable.size(); i++)
        {
            System.out.println("index:" + i + ", nodeID: " + fingerTable.get(i).nodeId + ", address:" + fingerTable.get(i).ip + ":" + fingerTable.get(i).port);
        }
        System.out.println("===============================================");
    }

    @Override
    public TableItem getSuccOf(long nodeID) throws org.apache.thrift.TException
    {
        TableItem predecessor = getPredOf(nodeID);

        TTransport transport = new TSocket(predecessor.ip, predecessor.port);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        Node.Client predClient = new Node.Client(protocol);
        transport.open();
        TableItem ans = predClient.getSucc();
        transport.close();

        return ans;
    }

    @Override
    public TableItem getSucc() throws org.apache.thrift.TException
    {
        return fingerTable.get(0);
    }

    @Override
    public TableItem getPredOf(long nodeID) throws org.apache.thrift.TException
    {
        long startnode = selfItem.nodeId
        long endnode = fingerTable.get(0).nodeId;
        TableItem ans = selfItem;
        while (!(isIn(nodeID, startnode, endnode) || nodeID == endnode))
        {
            //get cloest finger preceding id
            TTransport transport = new TSocket(ans.ip, ans.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            Node.Client client = new Node.Client(protocol);
            transport.open();
            ans = client.getClosestPredFinger(nodeID);
            transport.close();

            //get successor
            transport = new TSocket(ans.ip, ans.port);
            protocol = new TBinaryProtocol(new TFramedTransport(transport));
            client = new Node.Client(protocol);
            transport.open();
            TableItem succ = client.getSucc();
            transport.close();

            startnode = ans.nodeId;
            endnode = succ.nodeId;
        }
        return ans;
    }

    @Override
    public TableItem getClosestPredFinger(long nodeID) throws org.apache.thrift.TException
    {
        for(int i = fingerTable.size(); i >= 0; --i)
        {
            if (isIn(fingerTable.get(i).nodeId, selfItem.nodeId, nodeID))
            {
                return fingerTable.get(i);
            }
        }
        return selfItem;
    }

    @Override
    public TableItem getPred() throws org.apache.thrift.TException
    {
        return predecessor;
    }

    @Override
    public void setPred(TableItem pred) throws org.apache.thrift.TException
    {
        predecessor = pred;
    }
}

