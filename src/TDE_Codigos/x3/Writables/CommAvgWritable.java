package TDE_Codigos.x3.Writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.checkerframework.checker.units.qual.Temperature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommAvgWritable 
                implements WritableComparable<CommAvgWritable>{

    private int total;
    private float somaCommodity;

    public CommAvgWritable() {
    }

    public CommAvgWritable(float somaCommodity, int total) {
        this.total = total;
        this.somaCommodity = somaCommodity;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public float getSomaCommodity() {
        return somaCommodity;
    }
    
    public void setSomaCommodity(float somaCommodity) {
        this.somaCommodity = somaCommodity;
    }

    @Override
    public int compareTo(CommAvgWritable o) {
        // TODO Auto-generated method stub
        return Integer.compare( o.hashCode(), this.hashCode());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        // escrevendo em uma ordem desejada
        dataOutput.writeFloat(somaCommodity);
        dataOutput.writeInt(total);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // lendo na mesma ordem com que os dados foram escritos anteriormente
        somaCommodity = dataInput.readFloat();
        total = dataInput.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommAvgWritable that = (CommAvgWritable) o;
        return Float.compare(that.somaCommodity, somaCommodity) == 0 && total == that.total;
    }

    @Override
    public int hashCode() {
        return Objects.hash(somaCommodity, total);
    }
    
}
