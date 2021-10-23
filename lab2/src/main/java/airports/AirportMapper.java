package airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final String FIRST_STRING_PART = "Code";
    private static final String STRING_SPLITTER = "\",\"";
    private static final String EMPTY_STRING = "Code";
    private static final String DOUBLE_QUOTES = "Code";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        if (text.contains(FIRST_STRING_PART)) {
            return;
        }

        String[] values = text.split(STRING_SPLITTER);
        String airportID = values[0].replaceAll("\"", "");
        String airportName = values[1].replaceAll("\"", "");

        context.write(new AirportWritableComparable(airportID, 0), new Text(airportName));
    }

    private static String removeDoubleQuotes(String value) {
        return value.replaceAll("\"", EMPTY_STRING);
    }
}