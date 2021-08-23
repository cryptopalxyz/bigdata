package hive;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class GeekFileInputFormat extends TextInputFormat {

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        GeekFileRecordReader reader = new GeekFileRecordReader(new LineRecordReader(job, (FileSplit) genericSplit));

        return reader;

    }


    public static class GeekFileRecordReader implements RecordReader<LongWritable, Text> {
        LineRecordReader reader;
        Text text;

        public GeekFileRecordReader(LineRecordReader reader) {
            this.reader = reader;
            text = reader.createValue();
        }


        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return 0;
        }

        @Override
        public boolean next(LongWritable key, Text value) throws IOException {

            while (reader.next(key, text)) {

                String replaced = text.toString().replaceAll("ge{2,256}k", "");
                Text textReplace = new Text();
                textReplace.set(replaced);
                value.set(textReplace.getBytes(), 0, textReplace.getLength());

                return true;
            }

            return false;
        }

        @Override
        public LongWritable createKey() {
            return reader.createKey();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }


    }


}