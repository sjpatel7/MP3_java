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

public class TablePartA{

   public static void main(String[] args) throws IOException {
	//TODO
	//create table named "powers" in HBase with 3 column families (personal, professional, custom)
	//create table named "food" in HBase with 2 column families (nutrition, taste)
	//treate all data as String
	   
	//instantiations
	Configuration config = HBaseConfiguration.create();
	HBaseAdmin admin = new HBaseAdmin(config);
	HTableDescriptor tableDescriptor1 = new HTableDescriptor(TableName.valueOf("powers"));
	HTableDescriptor tableDescriptor2 = new HTableDescriptor(TableName.valueOf("food")); 
	//add column families
	tableDescriptor1.addFamily(new HColumnDescriptor("personal"));
	tableDescriptor1.addFamily(new HColumnDescriptor("professional"));
	tableDescriptor1.addFamily(new HColumnDescriptor("custom"));   
	tableDescriptor2.addFamily(new HColumnDescriptor("nutrition"));
	tableDescriptor2.addFamily(new HColumnDescriptor("taste")); 
	//Execute table through admin
	admin.createTable(tableDescriptor1);
	admin.createTable(tableDescriptor2);
   }
}

