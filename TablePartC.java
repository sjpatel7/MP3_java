import java.io.IOException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;

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

public class TablePartC{

   public static void main(String[] args) throws IOException {

	//TODO     
	//insert data from input.csv into powers table created from part A
	//schema is personal: hero, power ; professional: name, xp ; custom:  color
	Configuration config = HBaseConfiguration.create();
	HTable hTable = new HTable(config, "powers");
	
	try {
		BufferedReader br = new BufferedReader(new FileReader("input.csv"));
		//loop through each line in csv and add values for each row
		String line = "";
		while ((line = br.readLine()) != null) {
			String[] row = line.split(",");
			//instantiate put class, add values into put instance
			Put p = new Put(Bytes.toBytes(row[0]));
			//add values with add(column family name, qualifier/row name, value)
			p.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes(row[1]));
			p.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes(row[2]));
			p.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes(row[3]));
			p.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes(row[4]));
			p.add(Bytes.toBytes("custom"), Bytes.toBytes("color"), Bytes.toBytes(row[5]));
			
			hTable.put(new Put(p));
		}
		hTable.close();
	} catch (FileNotFoundException e) {
		hTable.close();
	} catch (IOException e) {
		hTable.close();
	}			       
   }
}

