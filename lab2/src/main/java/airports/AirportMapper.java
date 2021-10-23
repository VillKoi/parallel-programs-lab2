package airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final String FIRST_STRING_PART = "Code";

    private static final String STRING_SPLITTER = "\",\"";
    private static final String EMPTY_STRING = "";
    private static final String DOUBLE_QUOTES = "\"";

    private static final int AIRPORT_ID_NUMBER = 0;
    private static final int AIRPORT_NAME_NUMBER = 1;

    private static final int DIRECTORY_INDICATOR = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();
        if (text.contains(FIRST_STRING_PART)) {
            return;
        }

        String[] values = text.split(STRING_SPLITTER);
        String airportID = removeDoubleQuotes(values[AIRPORT_ID_NUMBER]);
        String airportName = removeDoubleQuotes(values[AIRPORT_NAME_NUMBER]);

        context.write(new AirportWritableComparable(airportID, DIRECTORY_INDICATOR), new Text(airportName));
    }

    private static String removeDoubleQuotes(String value) {
        return value.replaceAll(DOUBLE_QUOTES, EMPTY_STRING);
    }
}