import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;  
import java.util.Map.Entry; 
import java.util.Collections;
import java.util.List;
import java.util.Comparator;
import java.lang.*;


public class Recommendation{

	private static final Pattern numbers = Pattern.compile("(\\d+)");
	
	public static class UserVectorMap extends Mapper<Object, Text, IntWritable, Text>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			Matcher m = numbers.matcher(line);
		
			IntWritable userID = new IntWritable();
			String itemID;
			String pref;

			m.find();
			userID.set(Integer.parseInt(m.group()));
			m.find();
			itemID = m.group();
			m.find();
			pref = m.group();

			context.write(userID, new Text(itemID + ":" + pref));
		}
	}

	public static class UserVectorReduce extends Reducer<IntWritable, Text, Text, Text>{

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String outValue = new String();
			
			List<String> list = new ArrayList<String>();
			for(Text val : values)
				list.add(val.toString());

			Collections.sort(list, new Comparator<String>(){
				public int compare(String str1, String str2){
					int av = 0,bv = 0;
					int i = 0;
					while(str1.charAt(i) != ':'){
						av = av * 10 + str1.charAt(i) - '0';
						i++;	
					}
					i = 0;
					while(str2.charAt(i) != ':'){
						bv = bv * 10 + str2.charAt(i) - '0';
						i++;
					}
					return av - bv;
				}
			});

			for(String u : list)
				outValue += u + ",";

			context.write(new Text("U" + Integer.toString(key.get())),new Text(outValue));
		}
	}

	public static class CooccurrenceMap extends Mapper<Object, Text, IntWritable, IntWritable>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

			Pattern numbers = Pattern.compile("(\\d+)");
			ArrayList<String> itemIDArray = new ArrayList<String>();

			String line = value.toString();
			Matcher m = numbers.matcher(line);
			m.find();

		//	System.out.print(m.group()+": ");

			String itemID,pref;
			while(m.find()){
				itemID = m.group();
				m.find();
				pref = m.group();
				if(Integer.parseInt(pref) >= 4)
					itemIDArray.add(itemID);
			}

		//	System.out.println(Integer.toString(itemIDArray.size()));

			for(int i = 0; i < itemIDArray.size(); i++)
				for(int j = 0; j < itemIDArray.size(); j++)
					if(i != j)
						context.write(new IntWritable(Integer.parseInt(itemIDArray.get(i))), new IntWritable(Integer.parseInt(itemIDArray.get(j))));
		}
	}

	public static class CooccurrenceReduce extends Reducer<IntWritable, IntWritable, Text, Text>{
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			
			HashMap<Integer, Integer> hm = new HashMap<Integer, Integer>(256, 0.95f); 
			int tmp;

			for(IntWritable val : values){
				if(hm.get(val.get()) == null)
					hm.put(val.get(), 1);
				else{
					tmp = hm.get(val.get());
					hm.put(val.get(), tmp + 1);
				}
			}
			
			String outValue = new String();
			
			List<String> list = new ArrayList<String>();
			
     	   	Set<Entry<Integer, Integer>> sets = hm.entrySet();  
       		for(Entry<Integer, Integer> entry : sets) { 
				if(entry.getValue() >= 2) 
					list.add(Integer.toString(entry.getKey()) + ":" + Integer.toString(entry.getValue()));
        	} 

			Collections.sort(list, new Comparator<String>(){
				public int compare(String str1, String str2){
					int av = 0,bv = 0;
					int i = 0;
					while(str1.charAt(i) != ':'){
						av = av * 10 + str1.charAt(i) - '0';
						i++;	
					}
					i = 0;
					while(str2.charAt(i) != ':'){
						bv = bv * 10 + str2.charAt(i) - '0';
						i++;
					}
					return av - bv;
				}
			});

			for(String u : list)
				outValue += u + ",";
			
			context.write(new Text("C" + Integer.toString(key.get())), new Text(outValue));
		}
	}

	public static class MatrixMultMap extends Mapper<Object, Text, IntWritable, Text>{
		
		private int usersN;
		private int userNperReduce;
		private int reduceN;

		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			usersN = Integer.parseInt(conf.get("usersN"));
			userNperReduce = Integer.parseInt(conf.get("userNperReduce"));
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			int reduceKey;
			reduceN = usersN / userNperReduce + 1; 

			String line = value.toString();
			if(line.charAt(0) == 'C'){
				for(int i = 0; i < reduceN; i++)
					context.write(new IntWritable(i), value);
			}else{
				int userID = 0;
				int i = 1;
				while(line.charAt(i) != '\t'){
					userID = userID * 10 + line.charAt(i) - '0';
					i++;
				}
				reduceKey = userID / userNperReduce;
				context.write(new IntWritable(reduceKey), value);
			}	
		}
	}

	public static class UserPref{
		private int itemID;//,pref;
		float pref;
		
		public UserPref(int itemID, float pref){
			this.itemID = itemID;
			this.pref = pref;
		}
		
		public int getItemID(){
			return this.itemID;
		}
		public float getPref(){
			return this.pref;
		}
	}

	public static class MatrixMultReduce extends Reducer<IntWritable, Text, IntWritable, Text>{

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			ArrayList<Integer>[] coMatrix = new ArrayList[11000];
			for(int i = 0; i < 11000; i++)
				coMatrix[i] = new ArrayList<Integer>();

			ArrayList<Integer>[] userVector = new ArrayList[10001];
			for(int i = 0; i < 10001; i++)
				userVector[i] = new ArrayList<Integer>();

			int[] indexToItemID = new int[11000];
			int[] indexToUserID = new int[10001];
			int userN = 0, itemN = 0;
			
			int userID,itemID1,itemID2,sim,pref;

			for(Text val : values){
				String line = val.toString();
				Matcher m = numbers.matcher(line);
				if(line.charAt(0) == 'C'){
					m.find();
					itemID1 = Integer.parseInt(m.group());
					indexToItemID[itemN] = itemID1;
					while(m.find()){
						itemID2 = Integer.parseInt(m.group());
						m.find();
						sim = Integer.parseInt(m.group());
						coMatrix[itemN].add(itemID2);
						coMatrix[itemN].add(sim);
					}
					itemN++;
				}else{
					m.find();
					userID = Integer.parseInt(m.group());
					indexToUserID[userN] = userID;
					while(m.find()){
						itemID1 = Integer.parseInt(m.group());
						m.find();
						pref = Integer.parseInt(m.group());
						
					//	System.out.printf("userN:%d itemID:%d\n",userN,itemID1);

						userVector[userN].add(itemID1);
						userVector[userN].add(pref);
					}
					userN++;
				}
			}

			int[] itemSimSum = new int[11000];
			for(int i = 0; i < itemN; i++){
				itemSimSum[i] = 0;
				for(int ii = 0; ii < coMatrix[i].size(); ii++)
					itemSimSum[i] += coMatrix[i].get(ii);
				if(itemSimSum[i] == 0) itemSimSum[i] = 1;
			}

			ArrayList<UserPref>[] userPref = new ArrayList[10001];
			for(int i = 0; i < 10001; i++)
				userPref[i] = new ArrayList<UserPref>();

			for(int u = 0; u < userN; u++){
				for(int i = 0; i < itemN; i++){
					int itemPref = 0;
					int ii = 0, ui = 0;
					while(ii < coMatrix[i].size() && ui < userVector[u].size()){
						itemID1 = coMatrix[i].get(ii);
						itemID2 = userVector[u].get(ui);
						if(itemID1 == itemID2){
							itemPref += coMatrix[i].get(++ii) * userVector[u].get(++ui);
							ii++;
							ui++;
						}else if(itemID1 < itemID2) ii += 2;
						else if(itemID1 > itemID2) ui += 2;
					}
					UserPref upf = new UserPref(indexToItemID[i], (float)itemPref/itemSimSum[i]);
					userPref[u].add(upf);
				}
			}
			
			float minv,maxv;

			for(int u = 0; u < userN; u++)
				if(userPref[u].size() > 0){

					Collections.sort(userPref[u], new Comparator<UserPref>(){
						public int compare(UserPref up1, UserPref up2){
							if(up2.getPref() >= up1.getPref()) return 1;
							//else if(up2.getPref() == up1.getPref()) return 0;
							else return -1;
							//return up2.getPref() - up1.getPref();
						}
					});
					maxv = userPref[u].get(0).getPref();
					minv = userPref[u].get(userPref[u].size()-1).getPref();

					String outValue = new String();
					UserPref upf;
					for(int i = 0; i < userPref[u].size(); i++){
						upf = userPref[u].get(i);
						outValue += Integer.toString(upf.getItemID()) + ":" + Float.toString((upf.getPref() - minv)/(maxv - minv)) + ",";
					}
					context.write(new IntWritable(indexToUserID[u]), new Text(outValue));
				}
		}
	}
		
	public static void main(String[] args) throws Exception{

		Path userVectorDir = new Path("/user/lsw/recommendation/tmp/userVector");
		Path cooccurrenceMatrixDir = new Path("/user/lsw/recommendation/tmp/cooccurrenceMatrix");
		Path resultDir = new Path(args[3]);

		Configuration confUser = new Configuration();
		Job jobUser = new Job(confUser, "userVector");
		jobUser.setJarByClass(Recommendation.class);
		jobUser.setMapperClass(UserVectorMap.class);
		jobUser.setReducerClass(UserVectorReduce.class);
		
		jobUser.setMapOutputKeyClass(IntWritable.class);
		jobUser.setMapOutputValueClass(Text.class);
		jobUser.setOutputKeyClass(Text.class);
		jobUser.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(jobUser, new Path(args[2]));
		FileOutputFormat.setOutputPath(jobUser, userVectorDir);
		
		jobUser.waitForCompletion(true);
		
		Configuration confCo = new Configuration();
		Job jobCo = new Job(confCo, "cooccurrenceMatrix");
		jobCo.setJarByClass(Recommendation.class);
		jobCo.setMapperClass(CooccurrenceMap.class);
		jobCo.setReducerClass(CooccurrenceReduce.class);

		jobCo.setMapOutputKeyClass(IntWritable.class);
    jobCo.setMapOutputValueClass(IntWritable.class);
		jobCo.setOutputKeyClass(Text.class);
		jobCo.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(jobCo, userVectorDir);
		FileOutputFormat.setOutputPath(jobCo, cooccurrenceMatrixDir);
			
		jobCo.waitForCompletion(true);

		Configuration confMatrixM = new Configuration();
		confMatrixM.setStrings("usersN",args[0]);
		confMatrixM.setStrings("userNperReduce",args[1]);
		Job jobMatrixM = new Job(confMatrixM, "MatrixMultiply");
		jobMatrixM.setJarByClass(Recommendation.class);
		jobMatrixM.setMapperClass(MatrixMultMap.class);
		jobMatrixM.setReducerClass(MatrixMultReduce.class);

		jobMatrixM.setMapOutputKeyClass(IntWritable.class);
		jobMatrixM.setMapOutputValueClass(Text.class);
		jobMatrixM.setOutputKeyClass(IntWritable.class);
		jobMatrixM.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(jobMatrixM, userVectorDir);
		FileInputFormat.addInputPath(jobMatrixM, cooccurrenceMatrixDir);
		FileOutputFormat.setOutputPath(jobMatrixM, resultDir);

		jobMatrixM.waitForCompletion(true);
	}
}
