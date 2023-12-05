#include "Join.hpp"
//#include <iostream>
#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel,
                         pair<uint, uint> right_rel) {   

    // make the vector of buckets
    vector<Bucket> partitions;
    for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
        partitions.push_back(Bucket(disk));
    }

    // left_rel first
	    //hash relation R
	    // for each tuple r in R
		    // put r in bucket (output buffer) h1(r.p)
	    // flush B-1 output buffers to disk

    for (uint i = left_rel.first; i < left_rel.second; i++)
    {
        mem->loadFromDisk(disk, i, 0);

        for (uint j = 0; j < mem->mem_page(0)->size(); j++)
        {
            // hash the record
            Record r = mem->mem_page(0)->get_record(j);
            uint bufferID = r.partition_hash() % (MEM_SIZE_IN_PAGE - 1) + 1; //+ 1?
            
            // load record after checking if hash is alrdy full
            Page* hash = mem->mem_page(bufferID);
            if (hash->full())
            {
                partitions[bufferID - 1].add_left_rel_page(mem->flushToDisk(disk, bufferID));
            }
            hash->loadRecord(r);
        }
    }

    for (uint i = 1; i < MEM_SIZE_IN_PAGE; i++) {
        if (mem->mem_page(i)->size() != 0) { // > 0 ?
            partitions[i - 1].add_left_rel_page(mem->flushToDisk(disk, i));
        }
    }

    /*for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
        if (mem->mem_page(i)->size() > 0) {
            partitions[i].add_left_rel_page(mem->flushToDisk(disk, i));
        }
    }*/

    // right_rel second
        //hash relation S
        // for each tuple s in S
            // put s in bucket (output buffer) h1(s.o)
        // flush B-1 output buffers to disk
    for (uint i = right_rel.first; i < right_rel.second; i++)
    {
        mem->loadFromDisk(disk, i, 0);

        for (uint j = 0; j < mem->mem_page(0)->size(); j++)
        {
            // hash the record
            Record s = (mem->mem_page(0)->get_record(j));
            uint bufferID = s.partition_hash() % (MEM_SIZE_IN_PAGE - 1) + 1;

            // load record after checking if hash is alrdy full
            Page* hash = mem->mem_page(bufferID);
            if (hash->full())
            {
                partitions[bufferID - 1].add_right_rel_page(mem->flushToDisk(disk, bufferID));
            }

            hash->loadRecord(s);
        }
    }

    for (uint i = 1; i < MEM_SIZE_IN_PAGE; i++) {
        if (mem->mem_page(i)->size() > 0) { // > 0 ?
            partitions[i - 1].add_right_rel_page(mem->flushToDisk(disk, i));
        }
    }

    /*for (uint i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
        if (mem->mem_page(i)->size() > 0) {
            partitions[i].add_right_rel_page(mem->flushToDisk(disk, i));
        }
    }*/

    mem->reset();

    return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {

    vector<uint> probeResult;
    mem->reset();

    // Loop through each partition
    for (uint i = 0; i < partitions.size(); i++) {
        // Determine which is the smaller relation for this partition
        Bucket partition = partitions[i];
        bool is_left_smaller = partition.num_left_rel_record < partition.num_right_rel_record;

        vector<uint> smallerRel = is_left_smaller ? partition.get_left_rel() : partition.get_right_rel();
        vector<uint> largerRel = is_left_smaller ? partition.get_right_rel() : partition.get_left_rel();

        // Reset hash table memory pages
        for (uint b = 0; b < MEM_SIZE_IN_PAGE - 2; b++) {
            mem->mem_page(b)->reset();
        }

        // Build hash table with the smaller relation
        for (auto page_id : smallerRel) {
            mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 2);
            Page* input_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 2);

            for (uint r = 0; r < input_buffer->size(); ++r) {
                Record record = input_buffer->get_record(r);
                uint hash_val = record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                mem->mem_page(hash_val)->loadRecord(record);
            }
        }

        // Probe hash table with the larger relation
        for (auto page_id : largerRel) {
            mem->loadFromDisk(disk, page_id, MEM_SIZE_IN_PAGE - 2);
            Page* input_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 2);

            for (uint r = 0; r < input_buffer->size(); ++r) {
                Record probe_record = input_buffer->get_record(r);
                uint hash_val = probe_record.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                Page* matching_page = mem->mem_page(hash_val);

                for (uint s = 0; s < matching_page->size(); s++) {
                    Record match_record = matching_page->get_record(s);
                    if (probe_record == match_record) {
                        Page* output_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                        if (output_buffer->full()) {
                            uint flushed_disk_page = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
                            probeResult.push_back(flushed_disk_page);
                            output_buffer->reset();
                        }
                        output_buffer->loadPair(probe_record, match_record);
                    }
                }
            }
        }

        // Clear hash table memory pages for the next partition
        for (uint n = 0; n < MEM_SIZE_IN_PAGE - 2; n++) {
            mem->mem_page(n)->reset();
        }
    }

    // Flush remaining records in output buffer
    Page* output_buffer = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
    if (output_buffer->size() > 0) {
        uint flushed_disk_page = mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1);
        probeResult.push_back(flushed_disk_page);
    }

    return probeResult;
}

