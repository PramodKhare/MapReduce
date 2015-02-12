package cs6240.assignments.a1.v3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Class_Name: NaturalKeyGroupingComparator.java
 * @Purpose: Custom grouping comparator class which groups based on Natural Key component of
 *           composite key part.
 * @author Pramod Khare
 * @Created Tue 20 Jan 2015 01:45:24 PM EST
 * @Modified
 */

@SuppressWarnings("rawtypes")
public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
        super(PurchaseCategoryPriceCombinationKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        PurchaseCategoryPriceCombinationKey k1 = (PurchaseCategoryPriceCombinationKey) w1;
        PurchaseCategoryPriceCombinationKey k2 = (PurchaseCategoryPriceCombinationKey) w2;

        return k1.getPurchaseCategory().compareTo(k2.getPurchaseCategory());
    }
}
