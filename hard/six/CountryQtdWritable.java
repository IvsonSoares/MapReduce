package tde_grupo.hard.six;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CountryQtdWritable implements WritableComparable<CountryQtdWritable> {

    private String country;
    private double total;

    public CountryQtdWritable() {}

    public CountryQtdWritable(String country, double total) {
        this.country = country;
        this.total = total;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public double getTotal() {
        return total;
    }

    public void setTotal(double total) {
        this.total = total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountryQtdWritable that = (CountryQtdWritable) o;
        return Double.compare(that.total, total) == 0 && Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(country, total);
    }

    @Override
    public int compareTo(CountryQtdWritable o) {
        return Integer.compare(this.hashCode(), o.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country);
        dataOutput.writeDouble(total);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country = dataInput.readUTF();
        total   = dataInput.readDouble();
    }
}
