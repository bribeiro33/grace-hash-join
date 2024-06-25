# Grace Hash Join Implementation

The partition and probe functions of the Grace Hash Join (GHJ) algorithm are implemented to efficiently manage and join large data sets by leveraging disk and memory data structures. This project simulates the data flow of records in disk and memory and performs join operations between two relations.
Project Structure

The project consists of the following key components:

- Record: Defines the data structure for an emulated data record with key and data fields.
- Page: Defines the data structure for an emulated page containing multiple records.
- Disk: Defines the data structure for an emulated disk storing multiple pages.
- Mem: Defines the data structure for emulated memory managing pages in memory.
- Bucket: Defines the data structure for a bucket used in the partition phase to store disk page IDs and record counts for partitions.
- Join: Contains the implementation of the partition and probe functions that make up the two main stages of GHJ.

### Join
 - partition(Disk* disk, Mem* mem, pair<unsigned int, unsigned int> left_rel, pair<unsigned int, unsigned int> right_rel): Performs data record partition and outputs a vector of buckets.
 - probe(Disk* disk, Mem* mem, vector<Bucket>& partitions): Performs the probing and outputs a vector of disk page IDs of the join result.

### Building and Running

- How to build: make
- How to run: ./GHJ left_rel.txt right_rel.txt
- How to clean: make clean
