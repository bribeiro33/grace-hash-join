#include <iostream>

#include "Bucket.hpp"
#include "Join.hpp"
#include "Mem.hpp"

using namespace std;

void print(vector<uint>& join_res, Disk* disk) {
	cout << "Size of GHJ result: " << join_res.size() << " pages" << endl;
	for (uint i = 0; i < join_res.size(); ++i) {
		Page* join_page = disk->diskRead(join_res[i]);
		cout << "Page " << i << " with disk id = " << join_res[i] << endl;
		join_page->print();
	}
}

int main(int argc, char** argv) {
	/* Parse cmd arguments */
	if (argc != 3) {
		cerr << "Error: Wrong command line usage." << endl;
		cerr << "Usage: ./GHJ left_rel.txt right_rel.txt" << endl;
		exit(1);
	}

	/* Variable initialization */
	Disk disk;
	Mem mem;
	pair<uint, uint> left_rel = disk.read_data(argv[1]);
	pair<uint, uint> right_rel = disk.read_data(argv[2]);

	/* Grace Hash Join Partition Phase */
	vector<Bucket> res = partition(&disk, &mem, left_rel, right_rel);

	/* Grace Hash Join Probe Phase */
	vector<uint> join_res = probe(&disk, &mem, res);

	/* Print the result */
	print(join_res, &disk);
}
