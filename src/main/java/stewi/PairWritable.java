package stewi;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class PairWritable implements Writable {
    private LongWritable key;
    private Text value;

    public PairWritable(LongWritable key, Text value) {
        this.key = key;
        this.value = value;
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
        key = LongWritable.read(in);
        value = Text.read(in);
    }

    public static PairWritable read(DataInput in) throws IOException {
        PairWritable w = new PairWritable();
        w.readFields(in);
        return w;
    }
}

