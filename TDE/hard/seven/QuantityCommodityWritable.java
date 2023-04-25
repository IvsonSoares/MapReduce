package TDE.hard.seven;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class QuantityCommodityWritable implements WritableComparable<QuantityCommodityWritable> {

    private double quantity;
    private String commodity;

    public QuantityCommodityWritable() {}

    public QuantityCommodityWritable(double quantity, String commodity) {
        this.quantity = quantity;
        this.commodity = commodity;
    }

    public double getQuantity() {
        return quantity;
    }

    public void setQuantity(double quantity) {
        this.quantity = quantity;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuantityCommodityWritable that = (QuantityCommodityWritable) o;
        return quantity == that.quantity && Objects.equals(commodity, that.commodity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(quantity, commodity);
    }

    @Override
    public int compareTo(QuantityCommodityWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(quantity);
        dataOutput.writeUTF(commodity);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        quantity  = dataInput.readDouble();
        commodity = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "QuantityCommodity{" +
                "commodity='" + commodity + '\'' +
                "quantity='"  + quantity  + '\'' +
                '}';
    }
}
