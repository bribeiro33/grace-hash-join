/*
 * TODO: Implement partition, probe functions in Join.cpp
 */
#ifndef _JOIN_HPP_
#define _JOIN_HPP_

#include "Bucket.hpp"
#include "Mem.hpp"

/*
 * partition function
 *
 * Input:
 * disk: pointer of Disk object
 * mem: pointer of Memory object
 * left_rel: [left_rel.first, left_rel.second) will be the range of page ids of left relation to join
 * right_rel: [right_rel.first, right_rel.second) will be the range of page ids of right relation to join
 *
 * Output:
 * A vector of buckets of size (MEM_SIZE_IN_PAGE - 1).
 * Each bucket represent a partition of both relation.
 * See Bucket class for more information.
*/
std::vector<Bucket> partition(Disk* disk, Mem* mem,
                              std::pair<uint, uint> left_rel,
                              std::pair<uint, uint> right_rel);

/*
 * probe function
 * Input:
 * disk: pointer of Disk object
 * mem: pointer of Memory object
 * partition: a reference to a vector of buckets from partition function
 *
 * Output:
 * A vector of page ids that contains the join result.
*/
std::vector<uint> probe(Disk* disk, Mem* mem, std::vector<Bucket>& partition);

#endif
