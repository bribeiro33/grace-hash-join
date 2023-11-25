/*
 * This file defines the data structure for Data Record
 * DO NOT MODIFY THIS FILE
 */
#ifndef _RECORD_HPP_
#define _RECORD_HPP_

#include <string>

#include "constants.hpp"

class Record {
public:
	/* Constructor */
	Record(std::string _key, std::string _data);

	/* Copy constructor */
	Record(const Record& other);

	/* Hash value of key in the partition phase */
	uint partition_hash();

	/* Hash value of key in the probe phase*/
	uint probe_hash();

	/* Equality comparator */
	bool operator==(const Record& rhs) const;

	/* Print the key and data with in record*/
	void print();

	// The following functions are intended for debugging / grading
	bool operator<(const Record& rhs) const;
	bool equal(const Record& other);

private:
	std::string key;
	std::string data;
};

#endif
