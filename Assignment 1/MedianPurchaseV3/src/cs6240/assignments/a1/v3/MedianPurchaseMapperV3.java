package cs6240.assignments.a1.v3;

/**
 * @Purpose: MedianPurchaseMapper Mapper for finding median of purchases done in each category.
 * 
 * @FileName MedianPurchaseMapperV3.java
 * @author Pramod Khare
 * @Created Tue 20 Jan 2015 01:45:24 PM EST
 * @Modified
 */

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MedianPurchaseMapperV3 extends
        Mapper<LongWritable, Text, PurchaseCategoryPriceCombinationKey, FloatWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        String line = value.toString();
        // Split the line along the tab characters
        Pattern p = Pattern.compile("\\t+");
        try {
            String[] tokens = p.split(line);
            // First Token is Category name
            String strCategoryName = tokens[3];
            // Second Token is purchase price for that category
            String strPurchasePrice = tokens[4];

            float purchasePrice = Float.parseFloat(strPurchasePrice);

            context.write(new PurchaseCategoryPriceCombinationKey(strCategoryName, purchasePrice),
                    new FloatWritable(purchasePrice));
        } catch (final Throwable t) {
            // ignore the invalid inputs
        }
    }
}
