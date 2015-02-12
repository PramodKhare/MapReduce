package cs6240.assignments.a2.v3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @Class_Name: PurchaseCategoryPriceCombinationKey.java (Combination Key class for Purchase Price
 *              by Category.)
 * @Purpose: This is a Composite key class. The natural key is the Purchase Category value. The
 *           secondary sort will be performed against the purchase-price.
 * @author Pramod Khare
 * @Created Sun 08 Feb 2015 22:59:24 PM EST
 * @Modified
 */
public class PurchaseCategoryPriceCombinationKey implements
        WritableComparable<PurchaseCategoryPriceCombinationKey> {

    private String purchaseCategory;
    private int purchasePrice;

    public PurchaseCategoryPriceCombinationKey() {}

    public PurchaseCategoryPriceCombinationKey(final String purchaseCategory,
            final int purchasePrice) {
        this.purchaseCategory = purchaseCategory;
        this.purchasePrice = purchasePrice;
    }

    @Override
    public String toString() {
        return MessageFormat.format("'{'{0},{1}'}'", this.purchaseCategory, this.purchasePrice);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.purchaseCategory = WritableUtils.readString(in);
        this.purchasePrice = in.readInt();
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.purchaseCategory);
        out.writeInt(this.purchasePrice);
    }

    @Override
    public int compareTo(final PurchaseCategoryPriceCombinationKey key2) {
        int result = this.purchaseCategory.compareTo(key2.purchaseCategory);
        if (0 == result) {
            result =
                    (this.purchasePrice < key2.purchasePrice) ? -1
                            : ((this.purchasePrice == key2.purchasePrice) ? 0 : 1);
        }
        return result;
    }

    /********************************************************
     * Getters and Setters
     ********************************************************/
    public String getPurchaseCategory() {
        return purchaseCategory;
    }

    public void setPurchaseCategory(final String purchaseCategory) {
        this.purchaseCategory = purchaseCategory;
    }

    public int getPurchasePrice() {
        return purchasePrice;
    }

    public void setPurchasePrice(final int purchasePrice) {
        this.purchasePrice = purchasePrice;
    }
}
