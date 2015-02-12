package cs6240.assignments.a2.v4_new;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Class_Name: NaturalKeyPartitioner.java
 * @Purpose: Custom partitioner class which partitions based on Natural Key component of composite
 *           key part.
 * @author Pramod Khare, Srikar Reddy D
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified
 */
public class NaturalKeyPartitioner extends Partitioner<CategoryBinKey, BinCountValue> {

    @Override
    public int getPartition(CategoryBinKey key, BinCountValue val, int numPartitions) {
        int hash = key.getPurchaseCategory().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }
}
