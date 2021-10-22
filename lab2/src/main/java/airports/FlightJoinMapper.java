package airports;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightJoinMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // YEAR	QUARTER	MONTH	DAY_OF_MONTH	DAY_OF_WEEK	FL_DATE	UNIQUE_CARRIER	AIRLINE_ID	CARRIER	TAIL_NUM	FL_NUM	ORIGIN_AIRPORT_ID	ORIGIN_AIRPORT_SEQ_ID	ORIGIN_CITY_MARKET_ID	DEST_AIRPORT_ID	WHEELS_ON	ARR_TIME	ARR_DELAY	ARR_DELAY_NEW	CANCELLED	CANCELLATION_CODE	AIR_TIME	DISTANCE
        //2015	1	1	10	6	2015-01-10	AA	19805	AA	N790AA	1	12478	1247802	31703	12892	1225	1235	0.00	0.00	0.00		345.00	2475.00
//        < разбивает csv записываем нужную инфу
//            в контекст пишется пара — Text и IntWritable >
        // DEST_AEROPORT_ID имеет номер 14

        String text  =   value.toString();
        String[] values = text.split(",");
        String destAeroportID = values[14];

        context.write(new Text(destAeroportID), new Text(text));
    }
}