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

    //hash join for R and S
    // for k-1 to B-1 do
    for (uint i = 0; i < partitions.size(); i++) {

        // get smaller relation
        vector<uint> smallerRel;
        vector<uint> largerRel;
        
        if (partitions[i].num_left_rel_record < partitions[i].num_right_rel_record)
        {
            smallerRel = partitions[i].get_left_rel();
            largerRel = partitions[i].get_right_rel();
        }
        else
        {
            smallerRel = partitions[i].get_right_rel();
            largerRel = partitions[i].get_left_rel();
        }

        //for each tuple r in R
        for (uint j : smallerRel)
        {
            mem->loadFromDisk(disk, j, MEM_SIZE_IN_PAGE - 2);

            //put r in bucket h2(r.p)
            //for (uint k = 0; k < (mem->mem_page(0)->size()); k++) {
            //    Record r = mem->mem_page(0)->get_record(k);
            //    int hash = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
            //    // add r to the table in memory
            //    mem->mem_page(hash)->loadRecord(r);
            //}

            for (uint k = 0; k < mem->mem_page(MEM_SIZE_IN_PAGE - 2)->size(); k++) {
                Record r = mem->mem_page(MEM_SIZE_IN_PAGE - 2)->get_record(k);
                int hash = r.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                Page* hashPage = mem->mem_page(hash);

                if (hashPage->full()) {
                    // Assuming you have a method to add a page to a result or a way to manage full pages
                    probeResult.push_back(mem->flushToDisk(disk, hash));
                    hashPage->reset();
                }

                hashPage->loadRecord(r);
            }
        }
            
        //for each tuple s in S
        for (uint j : largerRel)
        {
            mem->loadFromDisk(disk, j, MEM_SIZE_IN_PAGE - 2);

            //for each tuple r in bucket h2(s.o)
            for (uint k = 0; k < (mem->mem_page(MEM_SIZE_IN_PAGE - 2)->size()); k++) {
                Record s = mem->mem_page(MEM_SIZE_IN_PAGE - 2)->get_record(k);
                int hash = s.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                Page* hashPage = mem->mem_page(hash);

                //if r.p == s.0
                for (uint o = 0; o < hashPage->size(); o++)
                {
                    Record r = hashPage->get_record(o);
                    // debugging statements to check the hash values
                    //r.print();
                    //s.print();
                    //std::cout << hash << std::endl;
                    //std::cout << r.probe_hash() % (MEM_SIZE_IN_PAGE - 2) << std::endl;
                    if (r == s)
                    {                     
                        //put (r, s) in the output relation
                        mem->mem_page(MEM_SIZE_IN_PAGE - 1)->loadPair(r, s);

                        // check if output page is full
                        if (mem->mem_page(MEM_SIZE_IN_PAGE - 1)->full())
                        { // dump output page when it is full
                            probeResult.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1));
                            //mem->mem_page(MEM_SIZE_IN_PAGE - 1)->reset();
                        }

                        
                    }
                }
            }
        }

        //clean up
        for (uint n = 1; n <= MEM_SIZE_IN_PAGE - 2; n++) {
            mem->mem_page(n)->reset();
        }
    }

    // final flush
    if (mem->mem_page(MEM_SIZE_IN_PAGE - 1)->size() > 0) {
        probeResult.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 1));
    }

    return probeResult;
}
