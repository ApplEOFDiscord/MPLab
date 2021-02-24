import java.io.File;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Recommender {
    public static int main(String[] args) throws Exception {
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Configuration ratingVectorConf = new Configuration();
        Job ratingVectorJob = Job.getInstance(ratingVectorConf, "RatingVector");
        ratingVectorJob.setJarByClass(Recommender.class);
        ratingVectorJob.setMapperClass(RatingVectorMapper.class);
        ratingVectorJob.setMapOutputKeyClass(LongWritable.class);
        ratingVectorJob.setMapOutputValueClass(MapWritable.class);
        ratingVectorJob.setReducerClass(RatingVectorReducer.class);
        ratingVectorJob.setOutputKeyClass(LongWritable.class);
        ratingVectorJob.setOutputValueClass(MapWritable.class);
        ratingVectorJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(ratingVectorJob, inputPath);
        FileOutputFormat.setOutputPath(ratingVectorJob, new Path(args[3] + File.separator + "temp_1"));

        Configuration cooccurrenceConf = new Configuration();
        Job cooccurrenceJob = Job.getInstance(cooccurrenceConf, "Cooccurrence");
        cooccurrenceJob.setJarByClass(Recommender.class);
        cooccurrenceJob.setMapperClass(CooccurrenceMapper.class);
        cooccurrenceJob.setMapOutputKeyClass(IntWritable.class);
        cooccurrenceJob.setMapOutputValueClass(IntWritable.class);
        cooccurrenceJob.setReducerClass(CooccurrenceReducer.class);
        cooccurrenceJob.setOutputKeyClass(IntWritable.class);
        cooccurrenceJob.setOutputValueClass(MapWritable.class);
        cooccurrenceJob.setInputFormatClass(SequenceFileInputFormat.class);
        cooccurrenceJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(cooccurrenceJob, new Path(args[3] + File.separator + "temp_1"));
        FileOutputFormat.setOutputPath(cooccurrenceJob, new Path(args[3] + File.separator + "temp_2"));

        Configuration ratingVectorTransformerConf = new Configuration();
        Job ratingVectorTransformerJob = Job.getInstance(ratingVectorTransformerConf, "RatingVectorTransformer");
        ratingVectorTransformerJob.setJarByClass(Recommender.class);
        ratingVectorTransformerJob.setMapperClass(RatingVectorTransformerMapper.class);
        ratingVectorTransformerJob.setMapOutputKeyClass(IntWritable.class);
        ratingVectorTransformerJob.setMapOutputValueClass(MapWritable.class);
        ratingVectorTransformerJob.setReducerClass(Reducer.class);
        ratingVectorTransformerJob.setOutputKeyClass(IntWritable.class);
        ratingVectorTransformerJob.setOutputValueClass(MapWritable.class);
        ratingVectorTransformerJob.setInputFormatClass(SequenceFileInputFormat.class);
        ratingVectorTransformerJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(ratingVectorTransformerJob, new Path(args[3] + File.separator + "temp_1"));
        FileOutputFormat.setOutputPath(ratingVectorTransformerJob, new Path(args[3] + File.separator + "temp_3"));

        Configuration toItemTransformerConf = new Configuration();
        Job toItemTransformerJob = Job.getInstance(toItemTransformerConf, "ToItemTransformer");
        toItemTransformerJob.setJarByClass(Recommender.class);
        toItemTransformerJob.setMapperClass(Mapper.class);
        toItemTransformerJob.setMapOutputKeyClass(IntWritable.class);
        toItemTransformerJob.setMapOutputValueClass(MapWritable.class);
        toItemTransformerJob.setReducerClass(ToItemTransformerReducer.class);
        toItemTransformerJob.setOutputKeyClass(IntWritable.class);
        toItemTransformerJob.setOutputValueClass(ItemWritable.class);
        toItemTransformerJob.setInputFormatClass(SequenceFileInputFormat.class);
        toItemTransformerJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(toItemTransformerJob, new Path(args[3] + File.separator + "temp_2"));
        FileInputFormat.addInputPath(toItemTransformerJob, new Path(args[3] + File.separator + "temp_3"));
        FileOutputFormat.setOutputPath(toItemTransformerJob, new Path(args[3] + File.separator + "temp_4"));

        Configuration recommendConf = new Configuration();
        Job recommendJob = Job.getInstance(recommendConf, "Recommend");
        recommendJob.setJarByClass(Recommender.class);
        recommendJob.setMapperClass(PartialMultiplyMapper.class);
        recommendJob.setMapOutputKeyClass(LongWritable.class);
        recommendJob.setMapOutputValueClass(MapWritable.class);
        recommendJob.setCombinerClass(AggregateCombiner.class);
        recommendJob.setReducerClass(RecommendReducer.class);
        recommendJob.setOutputKeyClass(LongWritable.class);
        recommendJob.setOutputValueClass(Text.class);
        recommendJob.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(recommendJob, new Path(args[3] + File.separator + "temp_4"));
        FileOutputFormat.setOutputPath(recommendJob, outputPath);

        Configuration conf = new Configuration();
        ControlledJob cRatingVectorJob = new ControlledJob(conf);
        ControlledJob cCooccurrenceJob = new ControlledJob(conf);
        ControlledJob cRatingVectorTransformerJob = new ControlledJob(conf);
        ControlledJob cToItemTransformerJob = new ControlledJob(conf);
        ControlledJob cRecommendJob = new ControlledJob(conf);

        cRatingVectorJob.setJob(ratingVectorJob);
        cCooccurrenceJob.setJob(cooccurrenceJob);
        cRatingVectorTransformerJob.setJob(ratingVectorTransformerJob);
        cToItemTransformerJob.setJob(toItemTransformerJob);
        cRecommendJob.setJob(recommendJob);

        cCooccurrenceJob.addDependingJob(cRatingVectorJob);
        cRatingVectorTransformerJob.addDependingJob(cRatingVectorJob);
        cToItemTransformerJob.addDependingJob(cCooccurrenceJob);
        cToItemTransformerJob.addDependingJob(cRatingVectorTransformerJob);
        cRecommendJob.addDependingJob(cToItemTransformerJob);

        JobControl jc = new JobControl("RecommenderControl");
        jc.addJob(cRatingVectorJob);
        jc.addJob(cCooccurrenceJob);
        jc.addJob(cRatingVectorTransformerJob);
        jc.addJob(cToItemTransformerJob);
        jc.addJob(cRecommendJob);

        Thread jcThread = new Thread(jc);
        jcThread.start();
        while(true){
            if(jc.allFinished()) {
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                return 0;
            }

            if(jc.getFailedJobList().size() > 0) {
                System.out.println(jc.getFailedJobList());
                jc.stop();
                return -1;
            }
        }
    }

    public static class RatingVectorMapper extends Mapper<LongWritable, Text, LongWritable, MapWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LongWritable outputKey = new LongWritable();
            MapWritable outputValue = new MapWritable();

            String[] record = value.toString().split(",");
            long userId = Long.valueOf(record[0]);
            outputKey.set(userId);
            int movieId = Integer.valueOf(record[1]);
            float rating = Float.valueOf(record[2]);
            outputValue.put(new IntWritable(movieId), new FloatWritable(rating));

            context.write(outputKey, outputValue);
        }
    }

    public static class RatingVectorReducer extends Reducer<LongWritable, MapWritable, LongWritable, MapWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable ratingVector = new MapWritable();
            for(MapWritable value: values) {
                for(Writable index: value.keySet()) {
                    IntWritable movieId = (IntWritable)index;
                    FloatWritable rating = (FloatWritable)value.get(index);
                    ratingVector.put(movieId, rating);
                }
            }

            context.write(key, ratingVector);
        }
    }

    public static class CooccurrenceMapper extends Mapper<LongWritable, MapWritable, IntWritable, IntWritable> {
        @Override
        protected  void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
           for(Writable first: value.keySet()) {
               for(Writable second: value.keySet()) {
                   IntWritable firstMovie = (IntWritable)first;
                   IntWritable secondMovie = (IntWritable)second;

                   context.write(firstMovie, secondMovie);
               }
           }
        }
    }

    public static class CooccurrenceReducer extends Reducer<IntWritable, IntWritable, IntWritable, MapWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable counter = new MapWritable();
            for(IntWritable value: values) {
                if(counter.containsKey(value)) {
                    IntWritable num = (IntWritable)counter.get(value);
                    counter.put(value, new IntWritable(num.get() + 1));
                }
                else {
                    counter.put(value, new IntWritable(1));
                }
            }

            context.write(key, counter);
        }
    }

    public static class RatingVectorTransformerMapper extends Mapper<LongWritable, MapWritable, IntWritable, MapWritable> {
        @Override
        protected  void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
            for(Writable index: value.keySet()) {
                MapWritable userRatingPair = new MapWritable();
                IntWritable movieId = (IntWritable)index;
                FloatWritable rating = (FloatWritable)value.get(index);
                userRatingPair.put(key, rating);

                context.write(movieId, userRatingPair);
            }
        }
    }

    public static class ToItemTransformerReducer extends Reducer<IntWritable, MapWritable, IntWritable, ItemWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<LongWritable> userIdList = new ArrayList<>();
            ArrayList<FloatWritable> ratingList = new ArrayList<>();
            MapWritable cooccurrenceVector = new MapWritable();

            for(MapWritable value: values) {
                for(Writable index: value.keySet()) {
                    if(index instanceof IntWritable) {
                        cooccurrenceVector.putAll(value);
                        break;
                    }
                    else {
                        LongWritable userId = (LongWritable)index;
                        userIdList.add(userId);
                        FloatWritable rating = (FloatWritable)value.get(index);
                        ratingList.add(rating);
                    }
                }
            }

            ItemWritable item = new ItemWritable(userIdList, ratingList, cooccurrenceVector);
            context.write(key, item);
        }
    }

    public static class PartialMultiplyMapper extends Mapper<IntWritable, ItemWritable, LongWritable, MapWritable> {
        @Override
        protected  void map(IntWritable key, ItemWritable value, Context context) throws IOException, InterruptedException {
            ArrayList<LongWritable> userIdList = value.getUserId();
            ArrayList<FloatWritable> ratingList = value.getRating();
            MapWritable cooccurrenceVector = value.getCooccurrence();

            MapWritable partialProduct = new MapWritable();
            for(int i = 0; i < userIdList.size(); i++) {
                LongWritable userId = userIdList.get(i);
                FloatWritable rating = ratingList.get(i);
                for(Writable index: cooccurrenceVector.keySet()) {
                    IntWritable elem = (IntWritable) cooccurrenceVector.get(index);
                    partialProduct.put((IntWritable)index, new FloatWritable(rating.get() * elem.get()));
                }

                 context.write(userId, partialProduct);
            }
        }
    }

    public static class AggregateCombiner extends Reducer<LongWritable, MapWritable, LongWritable, MapWritable> {
        @Override
        public void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable aggregateVector = new MapWritable();
            for(MapWritable singleVector: values) {
                for(Writable index: singleVector.keySet()) {
                    if(aggregateVector.containsKey(index)) {
                        FloatWritable oldValue = (FloatWritable)aggregateVector.get(index);
                        FloatWritable delta = (FloatWritable)singleVector.get(index);
                        aggregateVector.put((IntWritable)index, new FloatWritable(oldValue.get() + delta.get()));
                    }
                    else {
                        aggregateVector.put((IntWritable)index, singleVector.get(index));
                    }
                }
            }

            context.write(key, aggregateVector);
        }
    }

    public static class RecommendReducer extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable recommendVector = new MapWritable();
            for(MapWritable partialVector: values) {
                for(Writable index: partialVector.keySet()) {
                    if(recommendVector.containsKey(index)) {
                        FloatWritable oldValue = (FloatWritable)recommendVector.get(index);
                        FloatWritable delta = (FloatWritable)partialVector.get(index);
                        recommendVector.put((IntWritable)index, new FloatWritable(oldValue.get() + delta.get()));
                    }
                    else {
                        recommendVector.put((IntWritable)index, partialVector.get(index));
                    }
                }
            }

            Queue<RecommendItem> topItems = new PriorityQueue<>(Config.RecommendNumPerUser + 1, new RecommendItemComparator());
            for(Writable index: recommendVector.keySet()) {
                IntWritable movieId = (IntWritable)index;
                FloatWritable finalRating = (FloatWritable)recommendVector.get(index);
                if(topItems.size() < Config.RecommendNumPerUser) {
                    topItems.add(new RecommendItem(movieId.get(), finalRating.get()));
                }
                else if(finalRating.get() > topItems.peek().getRating()) {
                    topItems.add(new RecommendItem(movieId.get(), finalRating.get()));
                    topItems.poll();
                }
            }

            ArrayList<RecommendItem> recommendations = new ArrayList<>(topItems.size());
            recommendations.addAll(topItems);
            Collections.sort(recommendations, Collections.reverseOrder(new RecommendItemComparator()));

            StringBuilder recommendInfo = new StringBuilder();
            for(int i = 0; i < recommendations.size(); i++) {
                RecommendItem elem = recommendations.get(i);
                int movieId = elem.getMovieId();
                float rating = elem.getRating();
                recommendInfo.append(String.valueOf(movieId) + ":" + String.valueOf(rating));
                if(i != recommendations.size() - 1) recommendInfo.append(" ");
            }

            context.write(key, new Text(recommendInfo.toString()));
        }
    }
}