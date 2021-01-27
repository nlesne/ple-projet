package hbase;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

public class Utils {
	public final static String famName = "data";
	public final static String colName = "col";
	public final static String tablePrefix = "dauriac-lesne-";
	
	public static boolean createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		boolean existedBefore = admin.tableExists(table.getTableName());
		if (existedBefore) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
		return existedBefore;
	}

	public static boolean createTable(Connection connect, String tableName) {
		try {
			final Admin admin = connect.getAdmin();
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			boolean existed = admin.tableExists(tableDescriptor.getTableName());
			if (!existed) {
				HColumnDescriptor fam = new HColumnDescriptor(Bytes.toBytes(famName)); 
				tableDescriptor.addFamily(fam);
				admin.createTable(tableDescriptor);
			}
			admin.close();
			return existed;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
		return false;
	}

	public static byte[] getStoredValue(Connection connect, String tableName, String row) throws IOException {
		Table table = connect.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(row));
		Result res = table.get(get);
		return res.getValue(Bytes.toBytes(famName), Bytes.toBytes(colName));
	}
	
	public static void configRowPrefix(Configuration conf, String path) {
		Path inputPath = new Path(path);
		String[] dateParts = inputPath.getName().split("_");
		String rowDate = dateParts[1] + "_" + dateParts[2];
		conf.set("rowDate", rowDate);
	}
}
