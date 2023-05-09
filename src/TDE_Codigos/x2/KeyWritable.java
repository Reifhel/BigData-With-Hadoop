package TDE_Codigos.x2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class KeyWritable 
            implements WritableComparable<KeyWritable>{

    private String year;
    private String type;

    public KeyWritable() {
    }

    public KeyWritable(String year, String type) {
        this.year = year;
        this.type = type;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public int compareTo(KeyWritable o) {
        // TODO Auto-generated method stub
        return Integer.compare( o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // escrevendo em uma ordem desejada
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // lendo na mesma ordem com que os dados foram escritos anteriormente
        year = dataInput.readUTF();
        type = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyWritable that = (KeyWritable) o;
        return Objects.equals(year, that.year) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, type);
    }

    @Override
    public String toString() {
        return String.format("%s (%s):", type, year);
    }
    
}
