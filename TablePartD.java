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
import org.apache.hadoop.hbase.client.Get;


import org.apache.hadoop.hbase.util.Bytes;

public class TablePartD{

   public static void main(String[] args) throws IOException {

	// TODO      
	// DON' CHANGE THE 'System.out.println(xxx)' OUTPUT PART
	// OR YOU WON'T RECEIVE POINTS FROM THE GRADER 
	   
	//instantiate table
	Configuration config = HBaseConfiguration.create();
	HTable table = new HTable(config, "powers");
	   
	//Instantiate Get class for row 1 and 19
	Get g1 = new Get(Bytes.toBytes("row1"));
	Get g19 = new Get(Bytes.toBytes("row19"));  
	   
	//get id row1, (hero, power, name, xp, color)
	Result r1 = table.get(g1);
	byte [] value1 = r1.getValue(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	byte [] value2 = r1.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power"));
	byte [] value3 = r1.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name"));			 
	byte [] value4 = r1.getValue(Bytes.toBytes("professional"), Bytes.toBytes("xp"));
	byte [] value5 = r1.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));
					 
	String hero = Bytes.toString(value1);
	String power = Bytes.toString(value2);
	String name = Bytes.toString(value3);
	String xp = Bytes.toString(value4);
	String color = Bytes.toString(value5);
	System.out.println("hero: "+hero+", power: "+power+", name: "+name+", xp: "+xp+", color: "+color);
	
	//get id row19, (hero, color)
	Result r2 = table.get(g19);
	value1 = r2.getValue(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	value2 = r2.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));
	   
	hero = Bytes.toString(value1);
	color = Bytes.toString(value2);
	System.out.println("hero: "+hero+", color: "+color);

	//get id row1, (hero, name, coor)
	Result r3 = table.get(g1);
	value1 = r3.getValue(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
	value2 = r3.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name"));
	value3 = r3.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));
	   
	hero = Bytes.toString(value1);
	name = Bytes.toString(value2);
	color = Bytes.toString(value3);
	System.out.println("hero: "+hero+", name: "+name+", color: "+color); 
   }
}

