package mongodbtohdfs;

import java.util.Timer;
import java.util.TimerTask;


/**
 * @author Administrator
 *
 */
class MyTask extends TimerTask{
	private int i = 1;
	@Override
	public void run() {
		System.out.println("正在执行第："+i+" 次！");
		i++;
	}
}

public class testruneveryinternal {
	public static void main(String[] args) {
		Timer time = new Timer();
		time.schedule(new MyTask(), 1000, 120000);//1s之后开始执行，每2分钟执行一次，参数单位（毫秒）
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