package cs6240.assignments.a2.v3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Class_Name: NaturalKeyPartitioner.java
 * @Purpose: Custom partitioner class which partitions based on Natural Key component of composite
 *           key part.
 * @author Pramod Khare
 * @Created Sun 08 Feb 2015 22:59:24 PM EST
 * @Modified
 */
public class NaturalKeyPartitioner extends
        Partitioner<PurchaseCategoryPriceCombinationKey, IntWritable> {

    @Override
    public int getPartition(PurchaseCategoryPriceCombinationKey key, IntWritable val,
            int numPartitions) {
        int hash = key.getPurchaseCategory().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }
}
