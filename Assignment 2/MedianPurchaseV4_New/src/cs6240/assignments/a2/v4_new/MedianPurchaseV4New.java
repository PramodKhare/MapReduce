package cs6240.assignments.a2.v4_new;
/**
 * @FileName MedianPurchaseV4New.java (Map Reduce using composite keys to let MR do the sort
 *           Implementation along with different approach to find approximate median using bins and
 *           sampling rate )
 * @author Pramod Khare, Srikar Reddy D
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified
 * @Reference_Used: 
 *                  https://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-
 *                  hadoops -mapreduce-programming-paradigm/
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Configures the job and assigns mapper, reducer, comparator and partitioner classes to the job
 */

public class MedianPurchaseV4New {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: MedianPurchaseV4New <input path> <output path> <bin size> <sampling rate>");
            System.exit(-1);
        }

        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.setInt("BinSize", Integer.parseInt(args[2]));

        // Considering Sample Rate range - 0 to 100
        conf.setInt("SampleRate", Integer.parseInt(args[3]));

        Job job = new Job(conf);
        job.setJarByClass(MedianPurchaseV4New.class);
        job.setJobName("Approximate median V4 Binning Approach");

        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MedianPurchaseMapperV4.class);
        job.setCombinerClass(MedianPurchaseCombinerV4.class);
        job.setReducerClass(MedianPurchaseReducerV4.class);

        job.setMapOutputKeyClass(CategoryBinKey.class);
        job.setMapOutputValueClass(BinCountValue.class);

        job.setMapOutputKeyClass(CategoryBinKey.class);
        job.setMapOutputValueClass(BinCountValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();
        System.out.println("Total time - " + (endTime - startTime));
    }
}
