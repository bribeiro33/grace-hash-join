#include "Mem.hpp"

#include <iostream>

using namespace std;

/* RAII paradigm */
Mem::Mem() {
	/* Dynamic memory allocation for memory page */
	for (uint i = 0; i < MEM_SIZE_IN_PAGE; ++i) {
		pages.push_back(make_shared<Page>());
	}
}

Mem::~Mem() {
	for (auto& page : pages) {
		page->reset();
	}
}

void Mem::reset() {
	for (auto& page : pages) {
		page->reset();
	}
}

Page* Mem::mem_page(uint mem_page_id) { return pages[mem_page_id].get(); }

void Mem::loadFromDisk(Disk* d, uint disk_page_id, uint mem_page_id) {
	Page* disk_p = d->diskRead(disk_page_id);
	pages[mem_page_id]->loadPage(disk_p);
	num_load_from_disk++;
}

uint Mem::flushToDisk(Disk* d, uint mem_page_id) {
	uint new_disk_page_id = d->diskWrite(pages[mem_page_id]);
	pages[mem_page_id]->reset();
	num_flush_to_disk++;
	return new_disk_page_id;
}

void Mem::print() {
	for (uint i = 0; i < MEM_SIZE_IN_PAGE; i++) {
		cout << "PageID " << i << " in Mem:" << endl;
		if (pages[i]) {
			pages[i]->print();
		}
	}
}

size_t Mem::loadFromDiskTimes() const { return num_load_from_disk; }

size_t Mem::flushToDiskTimes() const { return num_flush_to_disk; }
