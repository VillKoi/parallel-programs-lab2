package airports;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //code — идентификатор аэропорта
        //description — название аэропорта
        String text  =   value.toString();
        String[] values = text.split(",");
        String code = values[0];
        String description = values[1];

        context.write(new Text(code), new Text(description));
    }
}
