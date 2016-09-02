package stewi;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;

public class WriteFile {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
//        conf.setLong("io.seqfile.compress.blocksize", 1000);
//        conf.setEnum("io.seqfile.compression.type", CompressionType.BLOCK);

        Path path = new Path(URI.create(args[0]));

        SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                Writer.file(path), Writer.keyClass(LongWritable.class),
                Writer.valueClass(Text.class),
                Writer.compression(CompressionType.BLOCK, new SnappyCodec()));

        for(int i=0; i<12345; i++) {
            writer.append(new LongWritable(i),
                    new Text(RandomStringUtils.randomAlphanumeric(100)));
            if(i%1000==0) writer.sync();
        }
        writer.hflush();
//        writer.close();
    }

}
