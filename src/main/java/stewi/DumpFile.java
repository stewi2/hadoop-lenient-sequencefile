package stewi;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class DumpFile {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(URI.create(args[0]));

        LenientSequenceFile.Reader reader =
                new LenientSequenceFile.Reader(conf,
                LenientSequenceFile.Reader.file(path));

//        SequenceFile.Reader reader =
//                new SequenceFile.Reader(conf,
//                SequenceFile.Reader.file(path));

        LongWritable key = new LongWritable();
        Text val = new Text();

        while (reader.next(key, val)) {
//            System.out.println(key + "\t" + val);
        }

        reader.close();
    }

}
