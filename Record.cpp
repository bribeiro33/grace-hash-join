#include "Record.hpp"

#include <iostream>

using namespace std;

#define MODULAR 1000000

Record::Record(string _key, string _data) {
	key = std::move(_key);
	data = std::move(_data);
}

Record::Record(const Record& other) {
	key = other.key;
	data = other.data;
}

/* h1 used at partition stage */
uint Record::partition_hash() {
	/* Use stl hash function */
	hash<string> str_hash;
	return str_hash(key) % MODULAR;
}

/* h2 different from h1 */
uint Record::probe_hash() {
	/* Use stl hash function */
	hash<string> str_hash;
	return str_hash("key:" + key) % MODULAR;
}

/* Equality comparator */
bool Record::operator==(const Record& rhs) const {
	hash<string> str_hash;
	/* If two records have different hash values(h2) of key, exit */
	if ((uint) (str_hash("key:" + key) % MODULAR) % (MEM_SIZE_IN_PAGE - 2)
	    != (uint) (str_hash("key:" + rhs.key) % MODULAR)
	            % (MEM_SIZE_IN_PAGE - 2)) {
		cout << "Error: Can not compare two records with different hash "
		        "values(rhs) of key."
		     << endl;
		exit(1);
	}
	return key == rhs.key;
}

void Record::print() {
	cout << "Record with key=" << key << " and data=" << data << "\n";
}

/* Less-than comparator */
bool Record::operator<(const Record& rhs) const {
	if (key != rhs.key) {
		return key < rhs.key;
	}
	return data < rhs.data;
}

/* Check if two records are equal */
bool Record::equal(const Record& other) {
	return key == other.key && data == other.data;
}
