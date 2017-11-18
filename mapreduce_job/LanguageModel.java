import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.*;


public class LanguageModel {

    public static class LanguageModelMap extends Mapper<LongWritable, Text, Text, Text> {

        private int threashold;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            threashold = conf.getInt("threashold", 20);
        }

        
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            if (value == null || value.toString().trim().length() == 0) {
                throw new IOException("Value is null or value contains nothing.");
            }

            String line = value.toString().trim();
            
            String[] wordsAndCount = line.split("\t");

            if (wordsAndCount.length < 2) {
                throw new IOException("The length of wordsAndCount is less than 2.");
            }
            
            String phrase = wordsAndCount[0].trim();
            int count = Integer.parseInt(wordsAndCount[1]);

            if (count < threashold) {
                return;
            }

            int lastSpaceIndex = phrase.lastIndexOf(" ");

            String starting_phrase = phrase.substring(0, lastSpaceIndex);
            String following_word = phrase.substring(lastSpaceIndex + 1);

            if (starting_phrase == null || starting_phrase.length() == 0) {
                throw new IOException("Starting phrase is null or its length equals to 0.");
            }

            context.write(new Text(starting_phrase), new Text(following_word + '=' + count));
        }
    }

    public static class LanguageModelReduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        private int topK;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();

            for (Text value : values) {
                String[] curVal = value.toString().split("=");
                String word = curVal[0].trim();
                Integer count = Integer.valueOf(curVal[1].trim());

                if (map.containsKey(count)) {
                    map.get(count).add(word);
                } else {
                    List<String> list = new ArrayList<String>();
                    list.add(word);
                    map.put(count, list);
                }
            }

            PriorityQueue<Node> heap = new PriorityQueue<Node>(map.size(), new Comparator<Node>() {
                @Override
                public int compare(Node e1, Node e2) {
                    return e2.count - e1.count;
                }
            });

            for (int count : map.keySet()) {
                heap.offer(new Node(count, map.get(count)));
            }

            for (int i = 0; i < topK && !heap.isEmpty(); ++i) {
                Node top = heap.poll();

                int keyCount = top.count;
                List<String> wordList = top.wordList;

                for (String word : wordList) {
                    context.write(new DBOutputWritable(key.toString(), word, keyCount), NullWritable.get());
                }
            }
        }

        class Node {
            public int count;
            public List<String> wordList;

            public Node(int count, List<String> wordList) {
                this.count = count;
                this.wordList = wordList;
            }
        }
    }

}