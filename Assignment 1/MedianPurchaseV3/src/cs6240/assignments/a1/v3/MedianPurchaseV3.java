package cs6240.assignments.a1.v3;

/**
 * @Problem_Desc: Input to program should accept a file with tab separated columns. The fourth
 *                column contains free form text describing an item category (for instance,
 *                "Children's toys") and the fifth a price in dollars and cents (for instance,
 *                12.33).
 * 
 *                Your program should compute the median of purchases by item category, and it
 *                should be robust to malformed inputs.
 * 
 * @FileName MedianPurchaseV3.java (Map Reduce using composite keys to let MR do the sort
 *           Implementation)
 * @author Pramod Khare
 * @Created Tue 20 Jan 2015 01:45:24 PM EST
 * @Modified Fri 30 Jan 2015 10:05:00 PM EST
 * @Reference_Used: 
 *                  https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-
 *                  hadoops -mapreduce-programming-paradigm/
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianPurchaseV3 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MedianPurchaseV3 <input path> <output path>");
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        Job job = new Job();
        job.setJarByClass(MedianPurchaseV3.class);
        job.setJobName("Find median purchase V3 using Composite Key and Secondary Sort");

        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MedianPurchaseMapperV3.class);
        job.setReducerClass(MedianPurchaseReducerV3.class);

        job.setMapOutputKeyClass(PurchaseCategoryPriceCombinationKey.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total time - " + (endTime - startTime));
    }
}
