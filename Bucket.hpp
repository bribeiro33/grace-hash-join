#ifndef _BUCKET_HPP_
#define _BUCKET_HPP_

#include "Disk.hpp"

class Bucket {
public:
	explicit Bucket(Disk* _disk);

	// return a list of page ids in disk from left relation in this bucket/partition
	std::vector<uint> get_left_rel();

	// return a list of page ids in disk from right relation in this bucket/partition
	std::vector<uint> get_right_rel();

	// add the disk page id of left relation into this bucket/partition
	void add_left_rel_page(uint page_id);

	// add the disk page id of right relation into this bucket/partition
	void add_right_rel_page(uint page_id);

	// number of records for left relation in this bucket
	// This is maintained by add_left_rel_page()
	uint num_left_rel_record = 0;

	// number of records from right relation in this bucket
	// This is maintained by add_right_rel_page()
	uint num_right_rel_record = 0;

private:
	// list of disk page ids contain the records from left relation in this bucket
	std::vector<uint> left_rel;

	// list of disk page ids contain the records from right relation in this bucket
	std::vector<uint> right_rel;

	// Pointer to the disk
	Disk* disk;
};

#endif
