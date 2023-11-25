#include "Disk.hpp"

#include <fstream>
#include <iostream>

using namespace std;

Disk::Disk() = default;

Disk::~Disk() {
	for (auto& page : pages) {
		page->reset();
	}
}

uint Disk::diskWrite(shared_ptr<Page>& p) {
	if (pages.size() == DISK_SIZE_IN_PAGE) {
		cerr << "Error: can not write to the disk due to out of disk space."
		     << endl;
		exit(1);
	}
	uint new_disk_page_id = pages.size();
	pages.push_back(make_shared<Page>(*p));
	return new_disk_page_id;
}

Page* Disk::diskRead(uint pos) {
	if (pos >= pages.size()) {
		cerr << "Error: accessing invalid disk page." << endl;
		exit(1);
	}
	return pages[pos].get();
}

void Disk::print(uint id) { pages[id]->print(); }

void Disk::print() {
	for (uint i = 0; i < pages.size(); i++) {
		if (pages[i]) {
			cout << "Disk page id: " << i << endl;
			pages[i]->print();
		}
	}
}

/* Different tables should not mix with each other */
pair<uint, uint> Disk::read_data(const char* filename) {
	/* Read all the raw data from txt file */
	string str_file_name(filename);
	ifstream raw_data_file(str_file_name);
	string one_line;
	uint start_page_id = pages.size();
	/* Create the first new disk page */
	pages.push_back(make_shared<Page>());
	while (getline(raw_data_file, one_line)) {
		if (pages.back()->full()) {
			/* Create a new disk page */
			pages.push_back(make_shared<Page>());
		}
		size_t space_idx = one_line.find(' ');
		string key = one_line.substr(0, space_idx);
		string data = one_line.substr(space_idx + 1);
		pages.back()->loadRecord(Record(key, data));
	}
	uint end_page_id = pages.size();
	raw_data_file.close();
	return make_pair(start_page_id, end_page_id);
}