service Node
{
	void setGenre(1: string title, 2: string genre, 3: bool withLogs),
	string getGenre(1: string title, 2: bool withLogs),
	void updateDHT(),
	void printDHT(),
}
