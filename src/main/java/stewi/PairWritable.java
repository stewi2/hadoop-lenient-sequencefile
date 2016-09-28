package stewi;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class PairWritable implements WritableComparable<PairWritable> {
    private LongWritable key = new LongWritable();
    private Text value = new Text();

    public PairWritable() {}

    public PairWritable(LongWritable key, Text value) {
        this.key.set(key.get());
        this.value.set(value);
    }

    public LongWritable getKey() {
        return key;
    }

    public Text getValue() {
        return value;
    }

    public void write(DataOutput out) throws IOException {
        key.write(out);
        value.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        value.readFields(in);
    }

    @Override
    public int hashCode() {
        return key.hashCode() + 31*value.hashCode();
    }

    @Override
    public int compareTo(PairWritable o) {
        // Primary sort on key, secondary sort on value
        if(key.equals(o.key)) {
            return value.compareTo(o.value);
        } else {
            return key.compareTo(o.key);
        }
    }
}

