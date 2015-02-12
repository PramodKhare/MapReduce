package cs6240.assignments.a1.v3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Class_Name: NaturalKeyPartitioner.java
 * @Purpose: Custom partitioner class which partitions based on Natural Key component of composite
 *           key part.
 * @author Pramod Khare
 * @Created Tue 20 Jan 2015 01:45:24 PM EST
 * @Modified
 */
public class NaturalKeyPartitioner extends
        Partitioner<PurchaseCategoryPriceCombinationKey, FloatWritable> {

    @Override
    public int getPartition(PurchaseCategoryPriceCombinationKey key, FloatWritable val,
            int numPartitions) {
        int hash = key.getPurchaseCategory().hashCode();
        int partition = hash % numPartitions;
        return partition;
    }
}
