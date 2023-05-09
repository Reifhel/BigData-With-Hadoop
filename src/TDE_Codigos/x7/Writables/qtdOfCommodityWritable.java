package TDE_Codigos.x7.Writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class qtdOfCommodityWritable 
    implements WritableComparable<qtdOfCommodityWritable>{

    private String commodity;
    private Float qtd;

    public qtdOfCommodityWritable() {
    }

    public qtdOfCommodityWritable(String commodity, Float qtd) {
        this.commodity = commodity;
        this.qtd = qtd;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public Float getQtd() {
        return qtd;
    }

    public void setQtd(Float qtd) {
        this.qtd = qtd;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(commodity);
        out.writeFloat(qtd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        commodity = in.readUTF();
        qtd = in.readFloat();
    }

    @Override
    public int compareTo(qtdOfCommodityWritable o) {
        return Float.compare(o.qtd, this.qtd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, qtd);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        qtdOfCommodityWritable other = (qtdOfCommodityWritable) obj;
        return Objects.equals(commodity, other.commodity) && Objects.equals(qtd, other.qtd);
    }

    @Override
    public String toString() {
        return String.format("| %s | %f", commodity, qtd);
    }
    
}
