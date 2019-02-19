package com.smily.dbtransfer;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.smily.fileUtil.PropertiesUtil;

public class TestMain {
	
	private static Logger log 	= Logger.getLogger(TestMain.class.getName());
	
	public static DbTool tool = new DbTool();
	
	public static int minKey = 0;
	public static int maxKey = 0;
	public static int rcd = -1;
	public static int startPrimaryKey = 0;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		PropertiesUtil.getProperties();
		
		String dbNameFrom = PropertiesUtil.srcDb_database;
		String tableNameFrom = PropertiesUtil.srcDb_table;
		String tableNameTo = PropertiesUtil.destDb_table;
		String keyName = PropertiesUtil.primaryKey;
		
		init( tool );		
		transfer( tool, dbNameFrom, tableNameFrom, tableNameTo, keyName );
		quit( tool );		
		endWhile();		
	}
	
	public static void transfer( DbTool tool, String dbNameFrom, String tableFrom, String tableTo, String keyName ){
		int recordPerQuery = PropertiesUtil.recordPerBatch;
		ArrayList<Integer> colType = new ArrayList<Integer>();
		ArrayList<String> columnName = tool.getAllColName(PropertiesUtil.srcDb_database, tableFrom, colType );
		int[] minMax = tool.getMinMaxKey( tableFrom, keyName );
		minKey = minMax[0];
		maxKey = minMax[1];
		log.info( tableFrom + ":min(" + keyName + ")=" + minKey + ",max(" + keyName + ")=" + maxKey );
		int circle = 0;
		log.info( "每次转移数据：" + recordPerQuery + "行。开始转移。。。。。。" );
		long beforeWhile = System.currentTimeMillis();
		startPrimaryKey = minKey;
		if( PropertiesUtil.pkStartValue>0 ){
			startPrimaryKey = PropertiesUtil.pkStartValue;//断点续传功能，从上次失败的行开始重新转移数据。
		}
		while( rcd!=0 ){
			circle++;
			long beforeRead = System.currentTimeMillis();
			ArrayList<ArrayList<String>> records 
				= tool.query( PropertiesUtil.srcDb_type, tableFrom, keyName, startPrimaryKey, recordPerQuery, columnName.size() );
			long afterRead = System.currentTimeMillis();
			int n = records.size();
			long beforeWrite = System.currentTimeMillis();
			tool.insert( tableTo, columnName, colType, records );
			long afterWrite = System.currentTimeMillis();			
			long readTime = afterRead-beforeRead;
			long writeTime = afterWrite-beforeWrite;
			long totalTime = readTime + writeTime;
			log.info( "第" + circle + "次循环：读耗时" 
					+ readTime + "毫秒，写耗时" + writeTime + "毫秒，读写耗时" + totalTime + "毫秒。" );			
			if( 0==n ){
				break;
			}
//			else if( circle==30 ){
//				rcd += records.size();
//				break;
//			}
			else{
				if( rcd==-1 ){
					rcd = records.size();
				}else{
					rcd += records.size();
				}				
			}
			if(rcd>0){
				startPrimaryKey = 1 + Integer.parseInt(records.get(records.size()-1).get(0));//最后一行记录的第一个字段的值加1，作为下次查询的起始值
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				log.error( e.getMessage() );
			}
		}
		long afterWhile = System.currentTimeMillis();
		log.info( "已完成" + rcd + "行数据的转移任务。总耗时" + (afterWhile-beforeWhile) + "毫秒。");
	}
	
	public static void endWhile(){
		if( startPrimaryKey>0 ){
			PropertiesUtil.updateProperties( "pkStartValue", startPrimaryKey + "" );
		}
		log.info( "已完成" + rcd + "行数据的转移任务。" );
		System.exit(0);
	}
	
	public static void lookup( DbTool tool, String dbName ){
		ArrayList<String> tables = tool.getAllTableName(dbName);
		log.info( "\r\n共有" + tables.size() + "个表。" );
		for( int i=0;i<tables.size();i++ ){
			String table = tables.get(i);
			ArrayList<Integer> colType = new ArrayList<Integer>();
			ArrayList<String> columnName = tool.getAllColName(dbName, table, colType );
			log.info( "\r\n" + table + "表有" + columnName.size() + "个字段：" );
			for( int j=0;j<columnName.size();j++ ){
				String column = columnName.get(j);
				int type = colType.get(j);
				switch(type){
				case 1:
					log.info( table + "." + column + "（字符串类型）" );
					break;
				case 2:
					log.info( table + "." + column + "（数字类型）" );
					break;
				case 3:
					log.info( table + "." + column + "（日期类型）" );
					break;
				default:
					break;
				}
			}
		}
	}
	
	public static void init( DbTool tool ){
		boolean isInitial = tool.init();
		if( isInitial ){
			log.info("Init OK!");			
		}else{
			log.info("Init ERROR!");
		}
	}
	
	public static void quit( DbTool tool ){
		boolean isQuit = tool.destroy();
		if( isQuit ){
			log.info("Quit OK!");			
		}else{
			log.info("Quit ERROR!");
		}
	}

}
