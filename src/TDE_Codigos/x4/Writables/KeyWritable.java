package TDE_Codigos.x4.Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class KeyWritable
            implements WritableComparable<KeyWritable> {

    private String year;
    private String unit_type;
    private String category;

    public KeyWritable() {
    }

    public KeyWritable(String year, String unit_type, String category) {
        this.year = year;
        this.unit_type = unit_type;
        this.category = category;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getUnit_type() {
        return unit_type;
    }

    public void setUnit_type(String unit_type) {
        this.unit_type = unit_type;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(category);
        out.writeUTF(unit_type);
        out.writeUTF(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        category = in.readUTF();
        unit_type = in.readUTF();
        year = in.readUTF();
    }

    @Override
    public int compareTo(KeyWritable o) {
       return Integer.compare( o.hashCode(), this.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyWritable that = (KeyWritable) o;
        return Objects.equals(year, that.year) &&
                Objects.equals(unit_type, that.unit_type) && 
                Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, unit_type, category);
    }

    @Override
    public String toString() {

        return String.format("%s | %s (%s):", category, unit_type, year);
    }
    
}
