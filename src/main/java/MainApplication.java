

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class MainApplication {

	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(env);
		
		String sqlquery=null;
		
		try {
			BufferedReader queryFile = new BufferedReader(
					new FileReader("/home/rtudoran/git/flink-test/src/main/resources/queryIn"));
			sqlquery = queryFile.readLine();
			queryFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		
		DataStream<Tuple6<Long,Integer,String,String,Long,Double>> inputStr = env.socketTextStream("127.0.0.1", 12341)
				.map(new MapFunction<String, Tuple6<Long,Integer,String,String,Long,Double>>() {
			@Override
			public Tuple6<Long,Integer,String,String,Long,Double> map(String values) throws Exception {
				
				 String [] items = values.split(",");
				 Long timestamp = Long.parseLong(items[0]);
				 Integer id = Integer.parseInt(items[1]);
			     String	 user = items[2];
			     String	 note = items[3];
			     Long specificNumber = Long.parseLong(items[4]);
			     Double amount = Double.parseDouble(items[5]);
				
				Tuple6<Long,Integer,String,String,Long,Double> inputTuple = new 
						Tuple6<>(timestamp,id,user,note,specificNumber,amount);
				
				return inputTuple;
			}
		});
		
		
		tableEnvironment.registerDataStream(
				"inputStream",
				inputStr, "timeevent,id,name,note,specificnumber,amount");
		
		Table result = tableEnvironment.sql(sqlquery);
		
		//we expect that the output will be 2 fields timestamp and double
		TypeInformation<Tuple2<Long, Double>> tpinf = new TypeHint<Tuple2<Long, Double>>() {
		}.getTypeInfo();
		
		DataStream<Tuple2<Long, Double>> saa = tableEnvironment.toDataStream(result, tpinf);

		saa.print();

		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		env.execute("sql on custom schema");
		
	}

}
