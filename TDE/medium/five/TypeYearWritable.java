package TDE.medium.five;

import TDE.easy.four.AvgCommodityUnitYearCategWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TypeYearWritable implements WritableComparable<TypeYearWritable> {

    private String type;

    private String year;

    public TypeYearWritable() {}

    public TypeYearWritable(String type, String year) {
        this.type = type;
        this.year = year;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
        TypeYearWritable that = (TypeYearWritable) o;
        return Objects.equals(type, that.type) && Objects.equals(year, that.year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, year);
    }

    @Override
    public int compareTo(TypeYearWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type = dataInput.readUTF();
        year = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "TypeYearWritable{" +
                "type='" + type + '\'' +
                ", year='" + year + '\'' +
                '}';
    }
}
