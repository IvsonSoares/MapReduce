package TDE.easy.three;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityYearWritable implements WritableComparable<CommodityYearWritable> {
    String commodity;
    String year;

    public CommodityYearWritable() {}

    public CommodityYearWritable(String commodity, String year) {
        this.commodity = commodity;
        this.year = year;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommodityYearWritable that = (CommodityYearWritable) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, year);
    }

    @Override
    public String toString() {
        return "CommodityYearWritable{" +
                "commodity='" + commodity + '\'' +
                ", year='" + year + '\'' +
                '}';
    }

    @Override
    public int compareTo(CommodityYearWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        year      = dataInput.readUTF();
    }
}
