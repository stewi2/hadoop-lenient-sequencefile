package stewi;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

public class DumpFile {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(URI.create(args[0]));
        FSDataInputStream in = path.getFileSystem(conf).open(path);

        LenientSequenceFile.Reader reader =
                new LenientSequenceFile.Reader(conf,
               LenientSequenceFile.Reader.stream(in));

//        SequenceFile.Reader reader =
//                new SequenceFile.Reader(conf,
//                SequenceFile.Reader.stream(in));

        LongWritable key = new LongWritable();
        Text val = new Text();

        int n=0;
        while (reader.next(key, val)) {
//            System.out.println(key + "\t" + val);
              n++;
        }

        System.err.println("Read "+n+" records");

        reader.close();
    }

}
