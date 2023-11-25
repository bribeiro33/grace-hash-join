#include "Page.hpp"

#include <iostream>

using namespace std;

Page::Page() = default;

Page::Page(const Page& other) { loadPage(&other); }

uint Page::size() { return records.size(); }

bool Page::empty() { return records.empty(); }

bool Page::full() { return records.size() == RECORDS_PER_PAGE; }

void Page::reset() { records.clear(); }

Record Page::get_record(uint record_id) { return records[record_id]; }

void Page::loadRecord(const Record& r) {
	if (records.size() < RECORDS_PER_PAGE) {
		records.emplace_back(r);
	} else {
		cout << "Error: Can not add record into full page." << endl;
		exit(1);
	}
}

// load 2 matching record into a page
// records per page will always be even number
void Page::loadPair(const Record& left_r, const Record& right_r) {
	if (records.size() >= RECORDS_PER_PAGE - 1) {
		cout << "Error: Can not add record into full page." << endl;
		exit(1);
	}
	records.push_back(left_r);
	records.push_back(right_r);
}

void Page::loadPage(const Page* p2) {
	reset();
	for (const auto& record : p2->records) {
		records.emplace_back(record);
	}
}

void Page::print() {
	for (auto& record : records) {
		record.print();
	}
}