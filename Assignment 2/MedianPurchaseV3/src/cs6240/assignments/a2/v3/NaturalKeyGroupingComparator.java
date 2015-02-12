package cs6240.assignments.a2.v3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Class_Name: NaturalKeyGroupingComparator.java
 * @Purpose: Custom grouping comparator class which groups based on Natural Key part/component of
 *           composite key.
 * @author Pramod Khare
 * @Created Sun 08 Feb 2015 22:59:24 PM EST
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
