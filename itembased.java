	import java.io.*;
     	import java.util.*;    
    	import java.lang.*;
    	import java.net.*;
    	import org.apache.hadoop.fs.Path;
    	import org.apache.hadoop.conf.*;
    	import org.apache.hadoop.io.*;
    	import org.apache.hadoop.mapred.*;
    	import org.apache.hadoop.util.*;
    	import org.apache.hadoop.filecache.*;       
    	import java.util.regex.*;





public class itembased  {
public static class matrixMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
	String line = value.toString();
	if(!(line.isEmpty()))
	{
	  //movie ratings are in the form "userID,movieID,rating,date"
          //each seperate <movieID,userID,rating,date> is delimited by a line break
          //tokenize the strings on ","
          StringTokenizer itr = new StringTokenizer(line);
          //String name to hold the movieID
          String useridAndrating=itr.nextToken();
          //set the movieID as the Key for the output <K V> pair
 
          //string to hold rating and date for each movie
          String movieid = itr.nextToken();
        word.set(movieid);  
	//get the date
          useridAndrating +="::"+itr.nextToken();
          //output the <movieID rating,date> to the reducer
          output.collect(word, new Text(useridAndrating));

     	}
  }
 
  
}

public static class matrixReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>  {

  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	String x="";
	int  first=1;
	int count=0;
	StringBuilder sb=new StringBuilder();
       

	while(values.hasNext()){
			
				x=values.next().toString();		    	
	    		
			if(first==1)
				{
				sb.append(x);			
				first=0;			
				}
			else		    	 
				{
				sb.append(","+x);
				}
		    
			
		}
		
			output.collect(key, new Text(sb.toString()));
			//output.collect(key, new Text(values.next().toString()));}
		
	}

}

public static class ratingCountMap extends MapReduceBase implements Mapper<LongWritable, Text,IntWritable, Text> {
	private Text word = new Text();
  public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	String [] temp;
	String line = value.toString();
	if(!(line.isEmpty()))
	{
	 
          StringTokenizer itr = new StringTokenizer(line);
          //discard the user id
          itr.nextToken();
          
          //string to hold rating and date for each user
          String movieidAndrating = itr.nextToken();
          String delimiter =",";
          temp = movieidAndrating.split(delimiter);
           IntWritable count=new IntWritable(temp.length);
          
	output.collect(count, new Text(movieidAndrating));

     	}
  }
 
  
}


public static class ratingCountReduce extends MapReduceBase implements Reducer<IntWritable, Text,IntWritable, Text>  {
	int i=0;
	
  public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
	IntWritable f= new IntWritable();
        
	
	while(values.hasNext()){
					Text word = values.next();	
					
					if(i<2){
				
					f.set(i);		    	
					output.collect(f,word);
				 			
				}
				i++;	
				
			}
		
			
			
	}

}
public static class centroidMap extends MapReduceBase implements Mapper<LongWritable, Text,Text,Text> {
	private Text word = new Text();
	private Text W = new Text();
		private Text F = new Text();
		private String fileName;
		private Path[] localFiles= new Path[0];
		private HashMap hm = new HashMap();
		
		public void configure(JobConf conf3)
		{
			try 
			{
				localFiles= DistributedCache.getLocalCacheFiles(conf3);
			} 
			catch (IOException ioe)
			{
				System.out.println(ioe.toString());
			}
		}
		
		
 	     	
		public static double calSimilarity(String temp,String input_words)
		{
                String[] temp1;
		String[] temp2;
                Vector movieid1 = new Vector();
                Vector movieid2 = new Vector();
                Vector rating1= new Vector();
                Vector rating2 = new Vector();
                
                double Rit=0.0,Rir=0.0;
                double Ar=0.0,Ai=0.0,val1=0.0,val2=0.0,sum1=0.0,sum2=0.0,num=0.0;
                StringTokenizer t1,t2,t3,t4,t5,t6;
                
                 String delimiter =",";
          	temp1 = temp.split(delimiter);
                                
                                for(int i=0;i<temp1.length;i++)
                                {
                                          t1 = new StringTokenizer(temp1[i],"::");
			
					movieid1.add(Integer.parseInt(t1.nextToken()));
                                       
                                        rating1.add(Double.parseDouble(t1.nextToken()));
                                        
                                        
                                }
                 delimiter =",";
          	temp2 = input_words.split(delimiter);
                                
                                for(int i=0;i<temp2.length;i++)
                                
                                {
					t2 = new StringTokenizer(temp2[i],"::");                                        
					movieid2.add(Integer.parseInt(t2.nextToken()));
                                        
                                        rating2.add(Double.parseDouble(t2.nextToken()));
                                        
                                        
                                }
                
                
                int i,j,count=0;
		Double value1=0.0,value2=0.0;
               
                for(i=0;i<movieid1.size();i++)
                {
                    int k = (Integer)movieid1.get(i);
                    Double h= (Double)rating1.get(i);
                    
                    for(j=0;j<movieid2.size();j++)
                    {
                        int l = (Integer)movieid2.get(j);
                        Double g = (Double)rating2.get(j);
                        
                       // System.out.println("l,h:"+l+","+h);
                        
                        if(k == l)
                        {
                            value1 += h;
                            value2 += g;
                            count++;
                            
                            
                        }
                    }
                    
                }
                Ai = (double)value1/count;
                Ar = (double)value2/count;
		
		double var1=0.0,var2=0.0;
                
                //System.out.println("Ai:"+Ai+","+"Ar:"+Ar);
                
                for(i=0;i<movieid1.size();i++)
                {
                    int k = (Integer)movieid1.get(i);
                    Rit = (Double) rating1.get(i);
                    
                    for(j=0;j<movieid2.size();j++)
                    {
                        int l = (Integer)movieid2.get(j);
                        Rir = (Double) rating2.get(j);
                        
                        	if(k == l)
                        		{
                            
                           		 var1 = Rit - Ai;
			   		 val1 = val1 + (var1*var1);
                            		System.out.println("val1:"+val1);
                           		 var2 = Rir - Ar;
			   		 val2 = val2 + (var2*var2);
			    		System.out.println("val2:"+val2);
                            		 num = var1 * var2;
			    		break;
			    	
                        		}
                   	 
				}//end of for movie id 2
		 	sum1 = sum1 + num; 
                    
                }
                
		System.out.println("num:"+num);
                double den = (val1*val2);
		
                den = Math.sqrt(den);
                if(den!=0)
		{
                double result = sum1/den;
                return result;
		}
		else
		return -99999;
     }




  public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {


	BufferedReader fr = new BufferedReader(new FileReader(localFiles[0].toString()));
			String input_words="";
 	       		
 	       		
		 	String temp1="",temp2="",cCentre="C2",centernumber="",userid="";
		 	double n = 0.0,max=-9999;
    	
		 	while((input_words = fr.readLine())!=null)
			{
			 		
			 	String line = value.toString();
				String delimiter = "\t";
				StringTokenizer tokens = new StringTokenizer(line,delimiter);
						    
				    while(tokens.hasMoreTokens())
				    {
				    	userid = tokens.nextToken();
				    	temp1 = tokens.nextToken();
				    	break;
				    }
				    StringTokenizer tokens1 = new StringTokenizer(input_words,delimiter);
				    while(tokens1.hasMoreTokens())
				    {
				   	centernumber = tokens1.nextToken();
				    	temp2 = tokens1.nextToken();
				    	break;
				    }
				    	//String x=userid+centernumber;
				    	n = calSimilarity(temp1,temp2);	
					
					//output.collect(new Text(x),new Text(Double.toString(n)));
					
		    		if(n>=max)
				{
				   	max = n;	
				 	cCentre = centernumber;
				}
				
				
		    
			 }
		 	String out = cCentre+"#"+temp1;
			output.collect(new Text(userid),new Text(out));
		                    	                            	                              
	 	 }
      }   

public static class centroidReduce extends MapReduceBase implements Reducer<Text, Text,Text, Text>  {
	int i=0;
	
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
        Text F = new Text();
		String line="";
		String artist_id="";
		int temp;
		

		  while (values.hasNext())
	          {
	
       		   F= values.next();			
		   line=F.toString();
		   output.collect(key,new Text(line));
		    	
		   
		  }
		  
		
		
		
 	   }
 	  }
public static class clusterMap extends MapReduceBase implements Mapper<LongWritable, Text,Text,Text> {
	

  public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
	String line = value.toString();
	String[] temp1,temp2;
	if(!(line.isEmpty()))
	{
	  //movie ratings are in the form "userID,movieID,rating,date"
          //each seperate <userid	clustercenter# {movieID::rating,movieid::rating}> is delimited by a line break
      
        	StringTokenizer itr = new StringTokenizer(line);
       
        	itr.nextToken();
          
          //string to hold rating and date for each movie
        	String movieidAndrating = "";
         
        	movieidAndrating = itr.nextToken();
          //get the date
        	String delimiter ="#";
        	temp1= movieidAndrating.split(delimiter);
	
		Text word=new Text(temp1[0]);
		delimiter=",";
		temp2=temp1[1].split(delimiter);
          
		for(int i=0;i<temp2.length;i++){
	          output.collect(word, new Text(temp2[i]));
		}
	   }                    	                            	                              
	 }
      }   

public static class clusterReduce extends MapReduceBase implements Reducer<Text, Text,Text, Text>  {
	int i=0;
	
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
        Text F = new Text();
	HashMap<String,Double> hm=new HashMap<String,Double>();	
	HashMap<String,Double> hmcount=new HashMap<String,Double>();		
	String temp="",ou="",movieid="",rating="";
	String[] t1,t2;
	String delimiter;
	double avg,var,d;		  
	


		while (values.hasNext())
	          {
			temp=values.next().toString();
			delimiter="::";
			t1=temp.split(delimiter);
			movieid=t1[0];
			rating=t1[1];


			if(hm.containsKey(movieid))
			{
			
				var=hm.get(movieid);			
				var=var+Double.parseDouble(rating);
				hm.put(movieid,var);	
				d=hmcount.get(movieid);
				d=d+1.0;
				hmcount.put(movieid,d);			
			}
			else{
				var=Double.parseDouble(rating);
				hm.put(movieid,var);	
				hmcount.put(movieid,new Double(1.0));			   
			 
			}			
			
       		}
		    	
		
        
       int flag=1;
             
        Iterator iterator = hm.keySet().iterator();
       
        while(iterator. hasNext()){        
			movieid=(String)iterator.next();           		
			var =hm.get(movieid);
        		d=hmcount.get(movieid);
			avg=(var/d);
			if(flag==1)		
			{
			 ou=movieid+"::"+Double.toString(avg);
			 flag=0;			
						
			}		
			else
			{
			ou=ou+","+movieid+"::"+Double.toString(avg);

			}	
					


		}		
		  
		
		output.collect(key,new Text(ou));
		
 	   }
 	  }


public static void main(String[] args) throws Exception {
		int i;
		JobConf conf1 = new JobConf(itembased.class);
		  conf1.setJobName("datamatrix");
		System.out.println("datamatrix");	
		conf1.setOutputKeyClass(Text.class);
		  conf1.setOutputValueClass(Text.class);
		 	
		  conf1.setInputFormat(TextInputFormat.class);
		  conf1.setOutputFormat(TextOutputFormat.class);

		  conf1.setMapperClass(matrixMapper.class);
		  conf1.setReducerClass(matrixReduce.class);
		 	
		 FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf1, new Path(args[1]+"/prgout0"));
			 	
		   JobClient.runJob(conf1);
		
		
		JobConf conf2 = new JobConf(itembased.class);
		  conf2.setJobName("Userrating count");
		 System.out.println("Userrating count");	
		  conf2.setOutputKeyClass(IntWritable.class);
		  conf2.setOutputValueClass(Text.class);
		 	
		  conf2.setInputFormat(TextInputFormat.class);
		  conf2.setOutputFormat(TextOutputFormat.class);
		conf2.setKeyFieldComparatorOptions("-r");
		  conf2.setMapperClass(ratingCountMap.class);
		  conf2.setReducerClass(ratingCountReduce.class);
		 	
		 FileInputFormat.setInputPaths(conf2, new Path(args[1]+"/prgout0"));
		 FileOutputFormat.setOutputPath(conf2, new Path(args[1]+"/centroid0"));
			 	
		   JobClient.runJob(conf2);
		for(i=0;i<3;i++){
		
		
		JobConf conf3 = new JobConf(itembased.class);
		  conf3.setJobName("centroid");
		 System.out.println("centroid"+i);
		 DistributedCache.addCacheFile(new URI(args[1]+"/centroid"+i+"/part-00000"),conf3);
		  conf3.setOutputKeyClass(Text.class);
		  conf3.setOutputValueClass(Text.class);
		 	
		  conf3.setInputFormat(TextInputFormat.class);
		  conf3.setOutputFormat(TextOutputFormat.class);
		conf3.setKeyFieldComparatorOptions("-r");
		  conf3.setMapperClass(centroidMap.class);
		  conf3.setReducerClass(centroidReduce.class);
		 	
		 FileInputFormat.setInputPaths(conf3, new Path(args[1]+"/prgout0"));
		 FileOutputFormat.setOutputPath(conf3, new Path(args[1]+"/result"+i));
			 	
		   JobClient.runJob(conf3);



		JobConf conf4 = new JobConf(itembased.class);
		  conf4.setJobName("cluster");
		  System.out.println("cluster"+i);
		  conf4.setOutputKeyClass(Text.class);
		  conf4.setOutputValueClass(Text.class);
		 	
		  conf4.setInputFormat(TextInputFormat.class);
		  conf4.setOutputFormat(TextOutputFormat.class);

		  conf4.setMapperClass(clusterMap.class);
		  conf4.setReducerClass(clusterReduce.class);
		 	
		 FileInputFormat.setInputPaths(conf4, new Path(args[1]+"/result"+i));
		 FileOutputFormat.setOutputPath(conf4, new Path(args[1]+"/centroid"+(i+1)));
			 	
		   JobClient.runJob(conf4);
		
 	}
  }
}

