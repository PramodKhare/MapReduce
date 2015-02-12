package cs6240.assignments.a1.v2;

/**
 * @Problem_Desc: Input to program should accept a file with tab separated columns. The fourth
 *                column contains free form text describing an item category (for instance,
 *                "Children's toys") and the fifth a price in dollars and cents (for instance,
 *                12.33).
 * 
 *                Your program should compute the median of purchases by item category, and it
 *                should be robust to malformed inputs.
 * 
 * @FileName MedianPurchaseV2.java (Map Reduce with sort in Reduce function)
 * @author Pramod Khare
 * @Created Mon 19 Jan 2015 11:45:24 PM EST
 * @Modified Wed 28 Jan 2015 10:45:24 AM EST
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianPurchaseV2 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MedianPurchaseV2 <input path> <output path>");
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        Job job = new Job();
        job.setJarByClass(MedianPurchaseV2.class);
        job.setJobName("Median purchase by category V2 using MapReduce basic version");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MedianPurchaseMapperV2.class);
        job.setReducerClass(MedianPurchaseReducerV2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total time - " + (endTime - startTime));
    }
}
