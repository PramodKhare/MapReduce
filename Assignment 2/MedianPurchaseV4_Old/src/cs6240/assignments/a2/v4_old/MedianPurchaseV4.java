package cs6240.assignments.a2.v4_old;

/**
 * @Problem_Desc: Input to program should accept a file with tab separated columns. The fourth
 *                column contains free form text describing an item category (for instance,
 *                "Children's toys") and the fifth a price in dollars and cents (for instance,
 *                12.33).
 * 
 *                Your program should compute the median of purchases by item category, and it
 *                should be robust to malformed inputs.
 * 
 * @FileName MedianPurchaseV4.java (Map Reduce where each call to map also computes Fibbonacci of
 *           N.)
 * @author Pramod Khare
 * @Created Mon 09 Feb 2015 12:59:24 PM EST
 * @Modified
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianPurchaseV4 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MedianPurchaseV4 <input path> <output path>");
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        Job job = new Job();
        job.setJarByClass(MedianPurchaseV4.class);
        job.setJobName("Median purchase by category V4 using Fibonacci in Mapper");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MedianPurchaseMapperV4.class);
        job.setReducerClass(MedianPurchaseReducerV4.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total time - " + (endTime - startTime));
    }
}
