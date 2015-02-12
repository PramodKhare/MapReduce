package cs6240.assignments.a1.v3;

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
 * @Created Tue 20 Jan 2015 01:45:24 PM EST
 * @Modified
 */
public class PurchaseCategoryPriceCombinationKey implements
        WritableComparable<PurchaseCategoryPriceCombinationKey> {

    private String purchaseCategory;
    private Float purchasePrice;

    public PurchaseCategoryPriceCombinationKey() {}

    public PurchaseCategoryPriceCombinationKey(String purchaseCategory, Float purchasePrice) {
        this.purchaseCategory = purchaseCategory;
        this.purchasePrice = purchasePrice;
    }

    @Override
    public String toString() {
        return MessageFormat.format("'{'{0},{1}'}'", this.purchaseCategory, this.purchasePrice);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.purchaseCategory = WritableUtils.readString(in);
        this.purchasePrice = in.readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, this.purchaseCategory);
        out.writeFloat(this.purchasePrice);
    }

    @Override
    public int compareTo(final PurchaseCategoryPriceCombinationKey key2) {
        int result = this.purchaseCategory.compareTo(key2.purchaseCategory);
        if (0 == result) {
            result = this.purchasePrice.compareTo(key2.purchasePrice);
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

    public Float getPurchasePrice() {
        return purchasePrice;
    }

    public void setPurchasePrice(final Float purchasePrice) {
        this.purchasePrice = purchasePrice;
    }
}
