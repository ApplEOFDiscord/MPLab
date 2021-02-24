import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Recommender {
    public static void main(String[] args) throws Exception {

    }

    public static class RatingVectorMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LongWritable outputKey = new LongWritable();
            Text outputValue = new Text();

            String[] record = value.toString().split(",");
            String userId = record[0];
            outputKey.set(Long.valueOf(userId));
            String movieId = record[1];
            String rating = record[2];
            outputValue.set(movieId + ":" + rating);

            context.write(outputKey, outputValue);
        }
    }

    public static class RatingVectorReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text outputValue = new Text();

            StringBuilder fullInfo = new StringBuilder();
            for(Text info: values) {
                fullInfo.append(info);
                fullInfo.append(" ");
            }
            outputValue.set(fullInfo.toString());
            context.write(key, outputValue);
        }
    }

    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected  void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] record = value.toString().split(" ");
            int len = record.length;
            for(int i = 0; i < len; i++) {
                for(int j = 0; j < len; j++) {
                    String[] first = record[i].split(":");
                    int firstMovie = Integer.valueOf(first[1]);

                    String[] second = record[j].split(":");
                    int secondMovie = Integer.valueOf(second[1]);

                    context.write(new IntWritable(firstMovie), new IntWritable(secondMovie));
                }
            }
        }
    }

    public static class CooccurrenceReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            HashMap<Integer, Integer> counter = new HashMap<>();

            for(IntWritable item: values) {
                int movieId = item.get();
                if(counter.containsKey(movieId)) {
                    int num = counter.get(movieId);
                    counter.put(movieId, num + 1);
                }
                else {
                    counter.put(movieId, 1);
                }
            }

            StringBuilder fullInfo = new StringBuilder();
            for(int movieID: counter.keySet()) {
                int num = counter.get(movieID);
                String info = String.valueOf(movieID) + ":" + String.valueOf(num);
                fullInfo.append(info);
                fullInfo.append(" ");
            }

            context.write(key, new Text(fullInfo.toString()));
        }
    }

    public static class RatingVectorTransformerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            long userId = key.get();
            String[] record = value.toString().split(" ");
            int len = record.length;
            for(int i = 0; i < len; i++) {
                String[] info = record[i].split(":");
                int movieId = Integer.valueOf(info[i]);
                context.write(new IntWritable(movieId), new Text(String.valueOf(userId) + ":" + info[1]));
            }
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

    public class AggregateCombiner extends Reducer<LongWritable, MapWritable, LongWritable, MapWritable> {
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

    public class RecommendReducer extends Reducer<LongWritable, MapWritable, LongWritable, Text> {
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