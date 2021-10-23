package airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final String FIRST_STRING_IDENTIFICATOR = "YEAR";

    private static final int AIRPORT_ID_NUMBER = 14;
    private static final int ARR_DELAY_NUMBER = 18;

    private static final String STRING_SPLITTER = ",";
    private static final String EMPTY_STRING = "";
    private static final String DOUBLE_QUOTES = "\"";

    private static final int DATA_INDICATOR = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        if (text.contains(FIRST_STRING_IDENTIFICATOR)) {
            return;
        }

        String[] values = text.split(STRING_SPLITTER);

        String destAirportID = removeDoubleQuotes(values[AIRPORT_ID_NUMBER]);
        String delayingTime = removeDoubleQuotes(values[ARR_DELAY_NUMBER]);

        if (delayingTime.isEmpty())  {
            return;
        }

        float delay = Float.parseFloat(delayingTime);

        if (delay == 0) {
            return;
        }

        context.write(new AirportWritableComparable(destAirportID, DATA_INDICATOR), new Text(String.valueOf(delay)));
    }

    private static String removeDoubleQuotes(String value) {
        return value.replaceAll(DOUBLE_QUOTES, EMPTY_STRING);
    }

    private static (String, boolean) correctDelayingTime(String value) {
        return value.replaceAll(DOUBLE_QUOTES, EMPTY_STRING);
    }
}