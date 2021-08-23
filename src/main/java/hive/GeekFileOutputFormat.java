package hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.Random;

public class GeekFileOutputFormat<K extends WritableComparable, V extends Writable>
        extends HiveIgnoreKeyTextOutputFormat<K, V> {

    public static class GeekFileRecordWriter implements RecordWriter {

        RecordWriter writer;
        Text text;

        public GeekFileRecordWriter(RecordWriter writer) {
            this.writer = writer;
            text = new Text();
        }


        @Override
        public void write(Writable writable) throws IOException {

            byte[] input;
            int inputLength;
            if (writable instanceof Text) {
                input = ((Text) writable).getBytes();
                inputLength = ((Text) writable).getLength();
            } else {
                assert (writable instanceof BytesWritable);
                input = ((BytesWritable) writable).getBytes();
                inputLength = ((BytesWritable) writable).getLength();
            }

            Random random = new Random();
            String line = new String(input);

            int index = random.nextInt(5) + 2;
            //System.out.println("index is :" + index );
            String [] split = line.split(" ");
            StringBuilder sb = new StringBuilder();
            int pre = 0;

            for (int i=0; i<split.length; i++) {
               // System.out.println("i is :" + i );
                if (i == pre + index) {
                    //System.out.println("pre is :" + pre + " and index is: " + index);
                    pre = i;
                    index = random.nextInt(5) + 2;
                    sb.append(" ");
                    sb.append(getGeekWord(i));
                }
                sb.append(" ");
                sb.append(split[i]);

            }

            String replaced = sb.toString();



            text.set(replaced.getBytes(), 0, replaced.getBytes().length);

            writer.write(text);

        }

        @Override
        public void close(boolean abort) throws IOException {
            writer.close(abort);
        }

        public static String getGeekWord(int n) {
            StringBuilder sb = new StringBuilder();
            sb.append("g");
            for (int i=0; i<n; i++) {
                sb.append("e");
            }
            sb.append("k");

            return sb.toString();
        }

    }

    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, Path outPath, Class<? extends Writable> valueClass, boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException {

        GeekFileRecordWriter writer = new GeekFileRecordWriter(super.getHiveRecordWriter(jc, outPath, BytesWritable.class, isCompressed,tableProperties, progress ));
        return writer;

    }

}