package mongodbtohdfs;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.Timer;
import java.util.TimerTask;


/**
 * @author Administrator
 *
 */
class MTHTask extends TimerTask{
	private int i = 1;
	 //在指定位置新建一个文件，并写入字符 
    public static void WriteToHDFS(String file, String words) throws IOException, URISyntaxException 
    { 
        Configuration conf = new Configuration(); 
        FileSystem fs = FileSystem.get(URI.create(file), conf); 
        Path path = new Path(file); 
        FSDataOutputStream out = fs.create(path);   //创建文件
       
        //两个方法都用于文件写入，好像一般多使用后者 
        //out.writeBytes(words);   
        out.write(words.getBytes("UTF-8")); 
         
        out.close(); 
        //如果是要从输入流中写入，或是从一个文件写到另一个文件（此时用输入流打开已有内容的文件） 
        //可以使用如下IOUtils.copyBytes方法。 
        //FSDataInputStream in = fs.open(new Path(args[0])); 
        //IOUtils.copyBytes(in, out, 4096, true)        //4096为一次复制块大小，true表示复制完成后关闭流 
    }

    public static void OneTimeMongotoHDFS(String colname){
    	//mongo中数据依据时间存入不同集合，于是查询时查询最新的集合
    	String timecol=colname;
        //获取连接
    	MongoClient client = new MongoClient("localhost", 27017); 
        //得到数据库
        MongoDatabase database = client.getDatabase("sparktest");
        //得到集合封装对象
        MongoCollection<Document> collection = database.getCollection(timecol);
        //得到查询结果
        FindIterable<Document> find = collection.find();
        int startYear=2015,endYear=2016;
        int startMonth=12,endMonth=1;
        int startDay=1,endDay=1;
        //遍历查询结果
        /*for(Document doc:find ){
            System.out.println("name:"+ doc.getString("name") );
            System.out.println("sex:"+doc.getString("sex"));
            System.out.println("age:"+doc.getDouble("age"));
            System.out.println("address:"+doc.getString("address"));
        }*/
        MongoCursor<Document> mongoCursor = find.iterator();  
		String file = "hdfs://localhost:9000/sparktest/"+timecol+".txt";
		String filecontext="";
		/*
         * 游标滚动-->获取记录-->读取字段值         
         */
        while(mongoCursor.hasNext()){  
            Document studentDocument = mongoCursor.next();
            System.out.println(studentDocument.getString("title"));
            //System.out.println(mongoCursor.next());  
            filecontext+=studentDocument.getString("title")+"\r\n"; // \r\n即为换行  
         }  
       
        //String words = "This words is to write into file!\n";
        try {
			WriteToHDFS(file, filecontext);
		} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	@Override
	public void run() {
		System.out.println("正在执行第："+i+" 次！");
		OneTimeMongotoHDFS("douban_col"+i);
		i++;
	}
}

public class mongotohdfseveryinternal {
	public static void main(String[] args) {
		Timer time = new Timer();
		time.schedule(new MTHTask(), 1000, 1200000);//1s之后开始执行，每20分钟执行一次，参数单位（毫秒）
		while(true){
			try {
				int in = System.in.read();//控制台输入t时停止定时器，具体定时器开关可根据实际业务需要自己设计
				if (in == 't') {
					time.cancel();//关闭定时器操作
					break;
				}
			} catch (Exception e) {
			}
		}
	}
}