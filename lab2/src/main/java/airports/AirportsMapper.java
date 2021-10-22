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
     //        < разбивает csv записываем нужную инфу
//  в контекст пишется пара — Text и IntWritable >
        String text  =   value.toString();
        String[] words =  text.replaceAll("[\\p{Punct}«».]", "").toLowerCase().split("\\s");
        for (String word: words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
