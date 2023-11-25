#include "Bucket.hpp"

using namespace std;

Bucket::Bucket(Disk* _disk) : disk(_disk) {}

vector<uint> Bucket::get_left_rel() { return left_rel; }

vector<uint> Bucket::get_right_rel() { return right_rel; }

void Bucket::add_left_rel_page(uint page_id) {
	left_rel.push_back(page_id);
	num_left_rel_record += disk->diskRead(page_id)->size();
}

void Bucket::add_right_rel_page(uint page_id) {
	right_rel.push_back(page_id);
	num_right_rel_record += disk->diskRead(page_id)->size();
}
