#ifndef _DISK_HPP_
#define _DISK_HPP_

#include "Page.hpp"

#include <memory>

class Disk {
public:
	Disk();

	~Disk();

	// Do not directly use this function in Join.cpp
	uint diskWrite(std::shared_ptr<Page>& p);

	// Do not directly use this function in Join.cpp
	Page* diskRead(uint pos);

	// Inspect the content of page in disk with specific id
	void print(uint id);

	// Inspect the content of all pages in disk
	void print();

	// Function used in main.cpp to load relation from txt file
	// Do not use this function in Join.cpp
	std::pair<uint, uint> read_data(const char* filename);

private:
	std::vector<std::shared_ptr<Page>> pages;
};

#endif