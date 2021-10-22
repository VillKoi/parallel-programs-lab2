package airports;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<AirportWritableComparable, Text, Text, Text> {
    @Override
    protected void reduce(AirportWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // На вход reducer подаются наборы данных в которых в первой строчке будет строка справочника,
        // а в последующих строки основной таблицы
        System.out.println("start reduce");

        Iterator<Text> iter = values.iterator();
        Text airportName = new Text(iter.next());
        float minTime = -1, maxTime = -1, meanTime = -1;
        int number = 0;

        while (iter.hasNext()) {
            Text delayingTime = iter.next();
            float delay = Float.parseFloat(delayingTime.toString());
            number++;

            meanTime += delay;
            minTime = Math.min(minTime, delay);
            maxTime = Math.max(maxTime, delay);
        }

        meanTime /= number;

        context.write(airportName, new Text(
                "Mean time:" + meanTime +
                        ", Max time: " + maxTime +
                        ", Min time: " + minTime));
    }
}