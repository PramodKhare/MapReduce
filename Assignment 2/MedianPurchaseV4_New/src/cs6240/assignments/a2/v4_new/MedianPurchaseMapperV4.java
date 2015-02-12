package cs6240.assignments.a2.v4_new;
/**
 * @Purpose: MedianPurchaseMapper Mapper for finding approximate median of purchases done in each
 *           category, it writes CategoryBin and its BinCount objects to the context as key value pair.
 *
 * @FileName MedianPurchaseMapperV4.java
 * @author Pramod Khare, Srikar Reddy D
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class MedianPurchaseMapperV4 extends
        Mapper<LongWritable, Text, CategoryBinKey, BinCountValue> {

    private static final Pattern p = Pattern.compile("\\t+");
    private static final int ONE = 1;

    private static int BIN_SIZE = 1;
    private static int SAMPLE_RATE = 100;

    private static Map<String, Integer> samplerCountsPerCategory = new HashMap<String, Integer>();

    /**
     * Called once at the beginning of the task.
     */
    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        configureParameters(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        // Split the line along the tab characters
        try {
            String[] tokens = p.split(value.toString());
            if (tokens.length < 5 || !shouldProcessLine(tokens[3])) {
                return;
            }
            // Fourth Token is Category name i.e. tokens[3]
            // Fifth Token is purchase price for that category i.e. tokens[4]
            // Then Decide which bin-number the value belongs to
            context.write(new CategoryBinKey(tokens[3], ((int) Float.parseFloat(tokens[4]) / BIN_SIZE)),
                    new BinCountValue(((int) Float.parseFloat(tokens[4]) / BIN_SIZE), ONE));
        } catch (final Throwable t) {
            // ignore the invalid inputs
        }
    }

    /**
     * Sampling decider logic
     *
     * @return boolean value - whether we select this sample or not
     */
    private boolean shouldProcessLine(final String category) {
        if (null == category) {
            return false;
        }

        int samplerCount = 0;
        if (samplerCountsPerCategory.containsKey(category)) {
            samplerCount = samplerCountsPerCategory.get(category);
        }

        samplerCount += SAMPLE_RATE;
        if (samplerCount < 100) {
            // Skip this line - this line is not sampled - by sampling routine
            samplerCountsPerCategory.put(category, samplerCount);
            return false;
        }
        samplerCount -= 100;
        samplerCountsPerCategory.put(category, samplerCount);
        return true;
    }

    /**
     * Extract global parameters and set them
     *
     * @param context context
     */
    private void configureParameters(
            Context context) {
        final Configuration conf = context.getConfiguration();
        BIN_SIZE = conf.getInt("BinSize", 1);
        SAMPLE_RATE = conf.getInt("SampleRate", 100);
    }
}
