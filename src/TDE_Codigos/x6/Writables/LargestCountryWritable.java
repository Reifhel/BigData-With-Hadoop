package TDE_Codigos.x6.Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class LargestCountryWritable 
    implements WritableComparable<LargestCountryWritable>{

    private String country;
    private Double value;


    public LargestCountryWritable() {
    }

    public LargestCountryWritable(String country, Double value) {
        this.country = country;
        this.value = value;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(country);
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country = in.readUTF();
        value = in.readDouble();
    }

    @Override
    public int compareTo(LargestCountryWritable o) {
       return Integer.compare( o.hashCode(), this.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LargestCountryWritable that = (LargestCountryWritable) o;
        return Objects.equals(country, that.country) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(country, value);
    }

    @Override
    public String toString() {
        return String.format("%s", country);
    }
    
}
