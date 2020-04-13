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

public class TestFindAlldemo {
	
	 //��ָ��λ���½�һ���ļ�����д���ַ� 
    public static void WriteToHDFS(String file, String words) throws IOException, URISyntaxException 
    { 
        Configuration conf = new Configuration(); 
        FileSystem fs = FileSystem.get(URI.create(file), conf); 
        Path path = new Path(file); 
        FSDataOutputStream out = fs.create(path);   //�����ļ�
       
        //���������������ļ�д�룬����һ���ʹ�ú��� 
        //out.writeBytes(words);   
        out.write(words.getBytes("UTF-8")); 
         
        out.close(); 
        //�����Ҫ����������д�룬���Ǵ�һ���ļ�д����һ���ļ�����ʱ�����������������ݵ��ļ��� 
        //����ʹ������IOUtils.copyBytes������ 
        //FSDataInputStream in = fs.open(new Path(args[0])); 
        //IOUtils.copyBytes(in, out, 4096, true)        //4096Ϊһ�θ��ƿ��С��true��ʾ������ɺ�ر��� 
    }

    public static void OneTimeMongotoHDFS(String colname){
    	//mongo����������ʱ����벻ͬ���ϣ����ǲ�ѯʱ��ѯ���µļ���
    	String timecol=colname;
        //��ȡ����
    	MongoClient client = new MongoClient("localhost", 27017); 
        //�õ����ݿ�
        MongoDatabase database = client.getDatabase("sparktest");
        //�õ����Ϸ�װ����
        MongoCollection<Document> collection = database.getCollection(timecol);
        //�õ���ѯ���
        FindIterable<Document> find = collection.find();
        int startYear=2015,endYear=2016;
        int startMonth=12,endMonth=1;
        int startDay=1,endDay=1;
        //������ѯ���
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
         * �α����-->��ȡ��¼-->��ȡ�ֶ�ֵ         
         */
        while(mongoCursor.hasNext()){  
            Document studentDocument = mongoCursor.next();
            System.out.println(studentDocument.getString("title") +", "+studentDocument.getString("description"));
            //System.out.println(mongoCursor.next());  
            filecontext+=studentDocument.getString("title") +", "+studentDocument.getString("description")+"\r\n"; // \r\n��Ϊ����  
         }  
       
        //String words = "This words is to write into file!\n";
        try {
			WriteToHDFS(file, filecontext);
		} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static void main(String[] args) {
    	OneTimeMongotoHDFS("col");
    }
}
