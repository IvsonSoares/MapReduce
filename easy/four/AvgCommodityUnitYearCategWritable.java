package TDE.easy.four;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class AvgCommodityUnitYearCategWritable implements WritableComparable<AvgCommodityUnitYearCategWritable> {

    String commodity;
    String unit;
    String year;
    String categ;

    public AvgCommodityUnitYearCategWritable() {}

    public AvgCommodityUnitYearCategWritable(String commodity, String unit, String year, String categ) {
        this.commodity = commodity;
        this.unit = unit;
        this.year = year;
        this.categ = categ;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getCateg() {
        return categ;
    }

    public void setCateg(String categ) {
        this.categ = categ;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvgCommodityUnitYearCategWritable that = (AvgCommodityUnitYearCategWritable) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(unit, that.unit) && Objects.equals(year, that.year) && Objects.equals(categ, that.categ);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, unit, year, categ);
    }

    @Override
    public String toString() {
        return "AvgCommodityUnitYearCategWritable{" +
                "commodity='" + commodity + '\'' +
                "unit='" + unit + '\'' +
                ", year='" + year + '\'' +
                ", categ='" + categ + '\'' +
                '}';
    }

    @Override
    public int compareTo(AvgCommodityUnitYearCategWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity);
        dataOutput.writeUTF(unit);
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(categ);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        unit  = dataInput.readUTF();
        year  = dataInput.readUTF();
        categ = dataInput.readUTF();
    }
}
