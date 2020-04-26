import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.util.Bytes;


public class TablePartF{

   public static void main(String[] args) throws IOException {

	// TODO      
	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER      

	//perform inner join sql query with hbase. join on color and diff name

	Configuration config = HBaseConfiguration.create();
	HTable table = new HTable(config, "powers");
	
	//part E scan code
	Scan scan = new Scan();
	scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("power"));
	scan.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("name"));
	scan.addColumn(Bytes.toBytes("custom"), Bytes.toBytes("color"));	     
	
	ResultScanner scanner = table.getScanner(scan);
	ResultScanner scanner2 = table.getScanner(new Scan(scan));
	for (Result result = scanner.next(); result != null; result = scanner.next()) {
		byte [] vName1 = result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name"));
		byte [] vPower1 = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power"));
		byte [] vColor = result.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));
		String name1 = Bytes.toString(vName1);
		String power1 = Bytes.toString(vPower1);
		String color = Bytes.toString(vColor);
		for (Result r2 = scanner2.next(); r2 != null; r2 = scanner2.next()) {
			byte [] vColor2 = r2.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));
			byte [] vName2 = r2.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name"));
			String color2 = Bytes.toString(vColor2);
			String name2 = Bytes.toString(vName2);
			if (color2.equals(color) && !name2.equals(name1)) {
				byte [] vPower2 = r2.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power"));
				String power2 = Bytes.toString(vPower2);
				System.out.println(name1 + ", " + power1 + ", " + name2 + ", " + power2 + ", "+color);
			}
		}
		
	}
	scanner.close();
	   
	/*   
	//cycle through each rowid lexicographically. match by color and diff name, then print
	String[] rowIDs = new String[25];
	for (int row = 1; row < 26; row++) {
		rowIDs[row - 1] = "row" + row;
	}
	for (String rowID : rowIDs) {
		Get g1 = new Get(Bytes.toBytes(rowID));
		Result r1 = table.get(g1);
		byte [] vName1 = r1.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name"));
		byte [] vPower1 = r1.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power"));
		byte [] vColor = r1.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));
		String name1 = Bytes.toString(vName1);
		String power1 = Bytes.toString(vPower1);
		String color = Bytes.toString(vColor);
		//might need to change order here from lex to int order
		for (String rowID2 : rowIDs) {
			Get g2 = new Get(Bytes.toBytes(rowID2));
			Result r2 = table.get(g2);
			byte [] vColor2 = r2.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));
			byte [] vName2 = r2.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name"));
			String color2 = Bytes.toString(vColor2);
			String name2 = Bytes.toString(vName2);
			if (color2.equals(color) && !name2.equals(name1)) {
				byte [] vPower2 = r2.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power"));
				String power2 = Bytes.toString(vPower2);
				System.out.println(name1 + ", " + power1 + ", " + name2 + ", " + power2 + ", "+color);
			}
		}
	}
	*/
	/*
	String name = ???;
	String power = ???;
	String color = ???;

	String name1 = ???;
	String power1 = ???;
	String color1 = ???;
	System.out.println(name + ", " + power + ", " + name1 + ", " + power1 + ", "+color);
	*/
   }
}
