/*
 * This file defines the data structure for Page
 * DO NOT MODIFY THIS FILE
 */

#ifndef _PAGE_HPP_
#define _PAGE_HPP_

#include <vector>

#include "Record.hpp"

class Page {
public:
	Page();

	/* Copy constructor */
	Page(const Page& other);

	/* Return number of records in this page */
	uint size();

	/* Return true if this page is empty */
	bool empty();

	/* Return true if this page is full of data records */
	bool full();

	/* Clear all the records in this page */
	void reset();

	/* Get the specific record in this->records at position record_id */
	Record get_record(uint record_id);

	/* Load single record into the page */
	void loadRecord(const Record& r);

	/* Load a pair of matching records in to this pages, consume 2 record spaces in this page*/
	void loadPair(const Record& left_r, const Record& right_r);

	/*
     * Load all data from page p2
     * Copy by value
     */
	void loadPage(const Page* p2);

	/* Print all the records info */
	void print();

private:
	std::vector<Record> records;
};

#endif