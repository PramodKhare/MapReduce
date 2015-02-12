package cs6240.assignments.a2.v4_new;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Class_Name: NaturalKeyGroupingComparator.java
 * @Purpose: Custom grouping comparator class which groups based on Natural Key component of
 *           composite key part.
 * @author Pramod Khare, Srikar Reddy D
 * @Created Sun 08 Feb 2015 23:22:21 PM EST
 * @Modified
 */

@SuppressWarnings("rawtypes")
public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(CategoryBinKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CategoryBinKey k1 = (CategoryBinKey) w1;
        CategoryBinKey k2 = (CategoryBinKey) w2;

        return k1.getPurchaseCategory().compareTo(k2.getPurchaseCategory());
    }
}
