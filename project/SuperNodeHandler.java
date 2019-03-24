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
    private int port;
    private boolean bIsJoining = false;
    private long numOfNodes;

    void setData(int port, long numOfNodes)
    {
        this.port = port;
        this.numOfNodes = numOfNodes;
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
        info.nack = false;

        bIsJoining = true;

        random node


        info.ip = ;
        info.port = ;
        return info;
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

//    @Override
//    public Result sendTask(List<String> filenames) throws org.apache.thrift.TException
//    {
//        mapResults.clear();
//        mapFilenames = filenames;
//        sortFinished = false;
//
//        // timer
//        long startTime = System.currentTimeMillis();
//
//        for (int i = 0; i < filenames.size(); ++i)
//        {
//            //if does not exist, exit
//            File file = new File(filenames.get(i));
//            if (!file.exists())
//            {
//                return null;
//            }
//
//            boolean flag = true;
//            while (flag) {
//                int idx = (int) (Math.random() * nodeList.size());
//
//                TTransport transport = new TSocket(nodeList.get(idx).IP, nodeList.get(idx).port);
//                TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
//                ComputeNode.Client nodeClient = new ComputeNode.Client(protocol);
//                transport.open();
//                if (nodeClient.mapTask(filenames.get(i))) {
//                    System.out.println("Map task successfully assigned on node "+ idx + ", address: " + nodeList.get(idx).IP + ":"+ nodeList.get(idx).port);
//                    flag = false;
//                }
//                transport.close();
//            }
//        }
//
//        try {
//            // waiting
//            while (!sortFinished) {
//                Thread.sleep(1);
//            }
//        } catch (Exception x) {
//            x.printStackTrace();
//        }
//
//        long timeUsed = System.currentTimeMillis() - startTime;
//
//        System.out.println("===============================================");
//        System.out.println("Job finished, map task number: "+ mapFilenames.size() +", time used: " + timeUsed + "ms");
//        System.out.println("===============================================");
//
//        Result res = new Result();
//        res.filename = resultFilename;
//        res.timeUsed = timeUsed;
//        return res;
//    }
//
//    @Override
//    public void noticeFinishedMap(String resultFilename) throws org.apache.thrift.TException
//    {
//        if(resultFilename.isEmpty())
//        {
//            System.out.println("Map job met with some error, check if file exists.");
//        }
//        else
//        {
//            System.out.println("Map job finish: " + resultFilename);
//        }
//
//
//        mapResults.add(resultFilename);
//
//        if (mapResults.size() == mapFilenames.size())
//        {
//            //sort
//            int idx = (int) (Math.random() * nodeList.size());
//
//            TTransport transport = new TSocket(nodeList.get(idx).IP, nodeList.get(idx).port);
//            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
//            ComputeNode.Client nodeClient = new ComputeNode.Client(protocol);
//            transport.open();
//            nodeClient.sortTask(mapResults);
//            transport.close();
//        }
//    }
}

