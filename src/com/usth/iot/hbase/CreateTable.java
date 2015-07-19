package com.usth.iot.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {

	private static final String TABLE_NAME = "hmbbs_logs";
	private static final String FAMILY_NAME = "cf";

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(TABLE_NAME));
		HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes(FAMILY_NAME));
		desc.addFamily(family);
		hBaseAdmin.createTable(desc);
		hBaseAdmin.close();
	}

}
