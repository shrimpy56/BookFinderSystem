struct TableItem
{
    1: string ip,
    2: i32 port,
    3: i64 nodeId,
}

struct NodeInfo
{
    1: string ip,
    2: i32 port,
    3: i64 nodeId,
    4: bool nack,
    5: i64 newNodeId,
    6: i64 maxNodeNum,
    //7: TableItem predecessor,
}

service SuperNode
{
    NodeInfo join(1: string ip, 2: i32 port),
    void postJoin(1: string ip, 2: i32 port),
    NodeInfo getNode()
}
