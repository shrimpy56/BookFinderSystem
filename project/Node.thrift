struct TableItem
{
    1: string ip,
    2: i32 port,
    3: i64 nodeId,
}

service Node
{
	void setGenre(1: string title, 2: string genre, 3: bool withLogs),
	string getGenre(1: string title, 2: bool withLogs),
	void updateDHT(1: TableItem nodeInfo, 2: i32 idx),
	void printDHT(),
	TableItem getSucc(),
    TableItem getSuccOf(1: i64 nodeID),
    TableItem getPred(),
    TableItem getPredOf(1: i64 nodeID),
    void setPred(1: TableItem pred),
    TableItem getClosestPredFinger(1: i64 nodeID),
    map<string, string> removeDataBeforeNode(1: i64 nodeID),
}
