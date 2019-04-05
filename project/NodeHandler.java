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
import java.security.*;
import java.util.Map.*;

public class NodeHandler implements Node.Iface
{
    private TableItem selfItem = new TableItem();
    private String serverIP;
    private int serverPort;
    private List<TableItem> fingerTable = new ArrayList<>();
    private Map<String, String> data;
    TableItem predecessor;
    private long maxNodeNum;

    private long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());

            return Math.abs(new BigInteger(md.digest()).intValue()) % maxNodeNum;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    boolean init(String serverIP, int serverPort, int port, NodeInfo helperNode)
    {
        System.out.println("start init..");

        try{
            this.serverIP = serverIP;
            this.serverPort = serverPort;
            selfItem.nodeId = helperNode.newNodeId;
            this.maxNodeNum = helperNode.maxNodeNum;

            selfItem.ip = InetAddress.getLocalHost().getHostAddress();
            selfItem.port = port;

            fingerTable.clear();
            int m = (int)(Math.log(maxNodeNum) / Math.log(2));
            //System.out.println("m = " + m);

            TableItem item = new TableItem();
            item.ip = helperNode.ip;
            item.port = helperNode.port;
            item.nodeId = helperNode.nodeId;

            //first node
            if (selfItem.nodeId == item.nodeId)
            {
                System.out.println("first node.");

                predecessor = item;

                for(int i = 0; i < m; i++)
                {
                    fingerTable.add(item);
                }
                data = new HashMap<>();
            }
            else
            {
                {
                    //find succ
                    TTransport transport = new TSocket(item.ip, item.port);
                    TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                    Node.Client helperClient = new Node.Client(protocol);
                    transport.open();
                    TableItem succ = helperClient.getSuccOf(selfItem.nodeId);
                    fingerTable.add(succ);

                    TTransport transport2 = new TSocket(succ.ip, succ.port);
                    TProtocol protocol2 = new TBinaryProtocol(new TFramedTransport(transport2));
                    Node.Client succClient = new Node.Client(protocol2);
                    transport2.open();
                    predecessor = succClient.getPred();
                    succClient.setPred(selfItem);
                    transport2.close();

                    for (int i = 1; i < m; i++) {
                        long nodeID = (long) (selfItem.nodeId + Math.pow(2, i)) % maxNodeNum;
                        long startnode = selfItem.nodeId;
                        long endnode = fingerTable.get(i - 1).nodeId;
                        if (isIn(nodeID, startnode, endnode) || nodeID == startnode) {
                            fingerTable.add(fingerTable.get(i - 1));
                        } else {
                            TableItem nodeItem = helperClient.getSuccOf(nodeID);
                            fingerTable.add(nodeItem);
                        }
                    }
                    transport.close();
                }

                //update
                for(int i = 0; i < m; i++)
                {
                    TableItem pred = getPredOf((long)(selfItem.nodeId - Math.pow(2, i) + maxNodeNum) % maxNodeNum);

                    if (!pred.ip.equals(selfItem.ip) || pred.port != selfItem.port)
                    {
                        System.out.println("update dht: pred of " + (long)(selfItem.nodeId - Math.pow(2, i) + maxNodeNum) % maxNodeNum + " is " + pred.nodeId);
                        //System.out.println("about to update dht of predecessor..");

                        TTransport transport = new TSocket(pred.ip, pred.port);
                        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                        Node.Client predClient = new Node.Client(protocol);
                        transport.open();
                        predClient.updateDHT(selfItem, i);
                        transport.close();
                    }
                }

                //System.out.println("start to move data..");

                //move key (pred, n]
                TTransport transport2 = new TSocket(fingerTable.get(0).ip, fingerTable.get(0).port);
                TProtocol protocol2 = new TBinaryProtocol(new TFramedTransport(transport2));
                Node.Client succClient = new Node.Client(protocol2);
                transport2.open();
                Map<String, String> newData = succClient.removeDataBeforeNode(selfItem.nodeId);
                transport2.close();
                data = newData;
            }

            //test
            System.out.println("init DHT:");
            printDHT();

            //post join
            System.out.println("sending post join..");
            TTransport transport = new TSocket(serverIP, serverPort);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SuperNode.Client serverClient = new SuperNode.Client(protocol);
            transport.open();
            serverClient.postJoin(selfItem.ip, port);
            transport.close();
        }catch(Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public Map<String, String> removeDataBeforeNode(long nodeID) throws org.apache.thrift.TException
    {
        Map<String, String> ans = new HashMap<>();
        for (Iterator<Map.Entry<String, String>> itr = data.entrySet().iterator(); itr.hasNext(); )
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

    boolean isIn(long nodeID, long startnode, long endnode)
    {
        if (startnode == endnode)
            return true;
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
                System.out.println("Set genre done on node " + selfItem.nodeId);

            data.put(title, genre);
        }
        else
        {
            TableItem finger = getSuccOf(key);

            if (withLogs)
                System.out.println("Forward set genre to node " + finger.nodeId);

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

//        if (withLogs)
//            System.out.println("get genre:key=" + key +",predecessor.nodeId="+ predecessor.nodeId+",selfItem.nodeId="+ selfItem.nodeId);

        if (isIn(key, predecessor.nodeId, selfItem.nodeId) || key == selfItem.nodeId)
        {
            if (withLogs)
                System.out.println("Getgenre done on node " + selfItem.nodeId);

            if (data.containsKey(title))
                return data.get(title);

            return "error: cannot find " + title;
        }
        else
        {
            TableItem finger = getSuccOf(key);

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
        System.out.println("update node " + nodeInfo.nodeId + " on idx " + idx + " of node " + selfItem.nodeId);

        //if (isIn(nodeInfo.nodeId, selfItem.nodeId, fingerTable.get(idx).nodeId) || nodeInfo.nodeId == selfItem.nodeId)
        long nodeID = (long) (selfItem.nodeId + Math.pow(2, idx)) % maxNodeNum;
        if (isIn(nodeID, fingerTable.get(idx).nodeId, nodeInfo.nodeId) || nodeID == nodeInfo.nodeId)
        {
            System.out.println("update idx " + idx + " ing.. ");

            fingerTable.set(idx, nodeInfo);

            //test
            printDHT();

            if ((predecessor.nodeId != selfItem.nodeId) && (nodeInfo.nodeId != predecessor.nodeId) )
            {
                System.out.println("sending update dht request to " + predecessor.nodeId);

                TTransport transport = new TSocket(predecessor.ip, predecessor.port);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                Node.Client predClient = new Node.Client(protocol);
                transport.open();
                predClient.updateDHT(nodeInfo, idx);
                transport.close();
            }

            System.out.println("update idx " + idx + " finish.. ");
        }
    }

    @Override
    public void printDHT() throws org.apache.thrift.TException
    {
        System.out.println("===============================================");
        System.out.println("Node ID: " + selfItem.nodeId);
        System.out.println("Range of keys: (" + predecessor.nodeId + ", " + selfItem.nodeId + "]");
        System.out.println("Data size: " + data.size());
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

        if (predecessor.ip.equals(selfItem.ip) && predecessor.port == selfItem.port)
        {
            return getSucc();
        }

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
        long startnode = selfItem.nodeId;
        long endnode = fingerTable.get(0).nodeId;
        TableItem ans = selfItem;
        while (!(isIn(nodeID, startnode, endnode) || nodeID == endnode))
        {
            if (ans.ip.equals(selfItem.ip) && ans.port == selfItem.port)
            {
                ans = getClosestPredFinger(nodeID);
            }
            else
            {
                //get cloest finger preceding id
                TTransport transport = new TSocket(ans.ip, ans.port);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                Node.Client client = new Node.Client(protocol);
                transport.open();
                ans = client.getClosestPredFinger(nodeID);
                transport.close();
            }

            System.out.println("Forward get predecessor to node " + ans.nodeId);

            TableItem succ = null;
            if (ans.ip.equals(selfItem.ip) && ans.port == selfItem.port)
            {
                succ = getSucc();
            }
            else
            {
                //get successor
                TTransport transport = new TSocket(ans.ip, ans.port);
                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                Node.Client client = new Node.Client(protocol);
                transport.open();
                succ = client.getSucc();
                transport.close();
            }

            startnode = ans.nodeId;
            endnode = succ.nodeId;
        }
        return ans;
    }

    @Override
    public TableItem getClosestPredFinger(long nodeID) throws org.apache.thrift.TException
    {
        for(int i = fingerTable.size() - 1; i >= 0; --i)
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

