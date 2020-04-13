package wc.w;
import com.kennycason.kumo.bg.PixelBoundryBackground;
import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.bg.RectangleBackground;
import com.kennycason.kumo.font.KumoFont;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.nlp.tokenizers.ChineseWordTokenizer;
import com.kennycason.kumo.palette.LinearGradientColorPalette;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;
import org.wltea.analyzer.core.*;
import org.apache.commons.lang.*;

import java.awt.*;
import java.io.IOException;
import java.io.StringReader;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
/**
 * Hello world!
 *
 */
public class App3 
{
	Map wordfre;
	//统计字符串的词频
	public static Map <String,Integer> setfre(String string){
		Map <String,Integer> wordfre = new HashMap <String,Integer> ();
		
		String [] arr = string.split("\n");
		try{
	        for(int i=0;i<arr.length;i++)
	        {
	        	String[] arr1=arr[i].split(" ");
	        	
	        	Integer num  =  Double.valueOf(arr1[1]).intValue();
	        	String bookname = "";
	        	for (int u=2;u<arr1.length;u++)
	        	{
	        		bookname += arr1[u]+' ';
	        	}
	        	System.out.println(bookname + "="+num.toString());
	        	wordfre.put(bookname, num);
	        }
		}
		catch(Exception e)
		{
		}
		return wordfre;
		}
	public static String readtxt2()
	{
		String ret="";
		String ret1="";
		String string[]=new String[3];
		File file=new File("D:\\doc1\\title_56.txt");
		try{
		List<String> line1=new ArrayList<String>();  
		BufferedReader br=new BufferedReader(new InputStreamReader(new FileInputStream(file),"GBK"));  
		String line = null;  
		
		while ((line = br.readLine()) != null) {  
		      //lines.add(line);  
		      //System.out.println(line);
		      ret += line+'\n';
		}   
		
		br.close();  
		}
		catch(Exception e)
		{
		}
		return ret;
	}



	 public static void main(String[] args) throws IOException {
		 	
		 	String str = App3.readtxt2();
		 	//System.out.println(str);
		 	// 获得词频的map
		 	Map <String,Integer> wordfre = App3.setfre(str);
		 	
	        final FrequencyAnalyzer frequencyAnalyzer = new FrequencyAnalyzer();
	        frequencyAnalyzer.setWordFrequenciesToReturn(600);
	        frequencyAnalyzer.setMinWordLength(2);
	        frequencyAnalyzer.setWordTokenizer(new ChineseWordTokenizer());
	        final List<WordFrequency> wordFrequencies = new ArrayList<>();
	        //生成wordFrequencies 
	        for(Map.Entry<String, Integer> entry : wordfre.entrySet()){
	            String mapKey = entry.getKey();
	            Integer mapValue = entry.getValue();
	            System.out.println(mapKey+":"+mapValue);
	            wordFrequencies.add(new WordFrequency(mapKey,mapValue));
	        }

	        //下面是生成词云的代码
	        java.awt.Font font = new java.awt.Font("STSong-Light", 2, 18);

	        final Dimension dimension = new Dimension(3000, 4000);
	        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
	        wordCloud.setPadding(2);
	        
	        wordCloud.setFontScalar(new SqrtFontScalar(12, 42));
	        
	        wordCloud.setColorPalette(new LinearGradientColorPalette(Color.blue, Color.black, Color.green, 30, 30));

	        wordCloud.setKumoFont(new KumoFont(font));
	        wordCloud.setBackgroundColor(new Color(255, 255, 255));
	        
	        Dimension dimension2 = new Dimension(1920, 1080);
	        wordCloud.setBackground(new CircleBackground(900));
	        
	       
	        wordCloud.build(wordFrequencies);
	        wordCloud.writeToFile("1.png");
	    }
}
