package airports;

import com.opencsv.exceptions.CsvException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

import com.opencsv.CSVReader;

public class AirportApp {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AirportsApp <1: input path FlightMapper> <2: input path AirportMapper> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(AirportApp.class);
        job.setJobName("JoinJob sort");

        List<String[]> list = ReadCSV(args[0]);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setPartitionerClass(AirportPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(AirportWritableComparable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static List<String[]> ReadCSV(String path) throws IOException, CsvException {
        Reader reader = new FileReader(path);
        CSVReader csvReader = new CSVReader(reader);

        List<String[]> list = csvReader.readAll();

        reader.close();
        csvReader.close();

        return list;
    }
}