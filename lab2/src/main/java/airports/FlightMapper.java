package airports;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, AirportWritableComparable, Text> {
    private static final String FIRST_STRING_IDENTIFICATOR = "YEAR";

    private static final int AIRPORT_ID = 14;
    private static final int ARR_DELAY = 18;


    private static final String STRING_SPLITTER = "\",\"";
    private static final String EMPTY_STRING = "";
    private static final String DOUBLE_QUOTES = "\"";

    private static final int AIRPORT_ID_NUMBER = 0;
    private static final int AIRPORT_NAME_NUMBER = 1;

    private static final int DIRECTORY_INDICATOR = 1;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String text = value.toString();

        if (text.contains(FIRST_STRING_IDENTIFICATOR)) {
            return;
        }

        String[] values = text.split(",");
        String destAirportID = values[AIRPORT_ID].replaceAll("\"", "");
        String delayingTime = values[ARR_DELAY].replaceAll("\"", "");

        if (delayingTime.equals("0.00")  || delayingTime.length() == 0)  {
            return;
        }

        float delay = Float.parseFloat(delayingTime);

        context.write(new AirportWritableComparable(destAirportID, 1), new Text(String.valueOf(delay)));
    }
}