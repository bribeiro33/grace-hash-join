/*
 * This files defines the data structure for Memory
 * DO NOT MODIFY THIS FILE
 */
#ifndef _MEM_HPP_
#define _MEM_HPP_

#include "Disk.hpp"
#include <memory>

class Mem {
public:
	Mem();

	~Mem();

	/* reset all memory pages */
	void reset();

	/* Returns the pointer to the memory page specified by page_id */
	Page* mem_page(uint mem_page_id);

	/* Load specific disk page to specific memory page */
	void loadFromDisk(Disk* d, uint disk_page_id, uint mem_page_id);

	/*
     * Write specific memory page into disk, and reset memory page
     * Return written disk page id
     */
	uint flushToDisk(Disk* d, uint mem_page_id);

	/* Print all the records info in Memory */
	void print();

	// The following functions are intended for debugging / grading
	size_t loadFromDiskTimes() const;
	size_t flushToDiskTimes() const;

private:
	std::vector<std::shared_ptr<Page>> pages;

	// The following member variables are intended for debugging / grading
	size_t num_load_from_disk = 0;
	size_t num_flush_to_disk = 0;
};

#endif