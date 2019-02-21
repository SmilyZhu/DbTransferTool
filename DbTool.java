package com.smily.dbtransfer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.smily.fileUtil.PropertiesUtil;

public class DbTool {
	
	private static Logger log 	= Logger.getLogger(DbTool.class.getName());
	
	private Connection conn4read = null;
	private Connection conn4write = null;;
	
	public boolean init(){
		boolean isReadable = getConnRead( PropertiesUtil.srcDb_driver, PropertiesUtil.srcDb_link, 
				PropertiesUtil.srcDb_user, PropertiesUtil.srcDb_password );
		boolean isWritable = getConnWrite( PropertiesUtil.destDb_driver, PropertiesUtil.destDb_link, 
				PropertiesUtil.destDb_user, PropertiesUtil.destDb_password );
		return  isReadable && isWritable;
	}
	
	private boolean getConnRead( String driver,String url,String user,String password ){
		try {
			Class.forName(driver);
			if( null==conn4read ){
				conn4read = DriverManager.getConnection(url,user,password);
			}
		} catch (ClassNotFoundException e) {
			log.error( e.getMessage() );
			return false;
		} catch (SQLException e) {
			log.error( e.getMessage() );
			return false;
		} 
		return true;
	}
	
	private boolean getConnWrite( String driver,String url,String user,String password ){
		try {
			Class.forName(driver);
			if( null==conn4write ){
				conn4write = DriverManager.getConnection(url,user,password);
			}
		} catch (ClassNotFoundException e) {
			log.error( e.getMessage() );
			return false;
		} catch (SQLException e) {
			log.error( e.getMessage() );
			return false;
		}
		return true;
	}
	
    
    /*
     * 原理：Column 3 is the TABLE_NAME (see documentation of getTables).
     */
    public ArrayList<String> getAllTableName( String databaseName ){
    	ArrayList<String> tableNameList = new ArrayList<String>();
    	DatabaseMetaData md = null;
    	ResultSet rs = null;
    	try{
	    	md = conn4read.getMetaData();
	    	rs = md.getTables(null, null, "%", null);
	    	while (rs.next()) {
//	    		String dbName = rs.getString(1);
	    		String schemaName = rs.getString(2);
	    		String tableName = rs.getString(3);
	    		if( schemaName.endsWith("dbo") ){
	    			tableNameList.add( tableName );
	    		}
	    	}
    	}catch(SQLException e ){
    		log.error( e.getMessage() );
    	}finally{
    		if(rs!=null){
    			try {
					rs.close();
				} catch (SQLException e) {
					log.error( e.getMessage() );
				}
    		}
    	}
    	return tableNameList;
    }
    
    /*
     * 原理：column 4是字段名
     */
    public ArrayList<String> getAllColName( String databaseName, String tableName, ArrayList<Integer> colType ){
    	ArrayList<String> colNameList = new ArrayList<String>();
    	DatabaseMetaData md = null;
    	ResultSet rs = null;
    	try{
	    	md = conn4read.getMetaData();
	    	rs = md.getColumns(databaseName, null, tableName, "%");
	    	while (rs.next()) {
//	    		String dbName = rs.getString(1);
//	    		String schemaName = rs.getString(2);
//	    		String tblName = rs.getString(3);
	    		String columnName = rs.getString(4);
	    		String columnType = rs.getString(6);
	    		columnType = columnType.toLowerCase();
	    		if( columnType.startsWith("varchar") || columnType.startsWith("nchar")
	    				|| columnType.startsWith("nvarchar") || columnType.startsWith("text") ){
	    			colType.add(1);
	    		}else if( columnType.startsWith("int") ){
	    			colType.add(2);
	    		}else if( columnType.startsWith("datetime") ){
	    			colType.add(3);
	    		}else if( columnType.startsWith("numeric") || columnType.startsWith("bigint") ){
	    			colType.add(4);
	    		}
	    		colNameList.add( columnName );
	    	}
    	}catch(SQLException e ){
    		log.error( e.getMessage() );
    	}finally{
    		if(rs!=null){
    			try {
					rs.close();
				} catch (SQLException e) {
					log.error(e.getMessage());
				}
    		}
    	}
    	return colNameList;
    }
    
    public int[] getMinMaxKey( String table, String keyName ){
    	int[] minMax = new int[2];
    	try {
			Statement state = conn4read.createStatement();
			String querySql = "select min(" + keyName + "), max(" + keyName + ") from " + table ;
			ResultSet result = state.executeQuery(querySql);
			while( result.next() ){
				minMax[0] = result.getInt( 1 );
				minMax[1] = result.getInt( 2 );				
			}
		} catch (SQLException e) {
			log.info( e.getMessage() );
		}
    	return minMax;
    }
    
    public ArrayList<ArrayList<String>> query( int dbType,String table, String keyName,int minKey,int lines,int numOfCol ){    	
    	log.info( "minKey=" + minKey + ",lines=" + lines + "." );    	
    	ArrayList<ArrayList<String>> recordList = new ArrayList<ArrayList<String>>();
    	try {
			Statement state = conn4read.createStatement();
			String querySql = "";
			switch(dbType){
			case 1://SQLServer
				querySql = "select top " + lines + " * from " + table + " where " + keyName + " >= " + minKey + " order by " + keyName;
				break;
			case 2://MySQL
				querySql = "select * from " + table + " where " + keyName + " >= " + minKey  + " order by " + keyName + " limit " + lines; 
				break;
			case 3://Oracle
				break;
			default:
				break;
			}
			log.info( "执行查询：" + querySql );
			ResultSet result = state.executeQuery( querySql );
			while( result.next() ){
				ArrayList<String> oneRecord = new ArrayList<String>();
				for( int i=0;i<numOfCol;i++ ){					
					oneRecord.add( result.getString((i+1)) );
				}
				recordList.add( oneRecord );
			}
			log.info( "查询到" + recordList.size() + "行记录。" );
			result.close();
			state.close();
		} catch (SQLException e) {
			log.error( e.getMessage() );
			TestMain.endWhile();
		} catch (Exception e) {
			log.error( e.getMessage() );
			TestMain.endWhile();
		}    	
    	return recordList;    	
    }
    
    public void insert( String table,ArrayList<String> colName,ArrayList<Integer> colType,ArrayList<ArrayList<String>> records){
    	try{
    		conn4write.setAutoCommit(false);
    		String insertSql_1 = "insert into " + table + " (";    		
    		String insertSql_2 = ") values(";
    		for(int i=0;i<colName.size();i++){
    			insertSql_1 += colName.get(i)+",";
    			insertSql_2 += "?,";
    		}
    		insertSql_1 = insertSql_1.substring(0,insertSql_1.length()-1);
    		insertSql_2 = insertSql_2.substring(0,insertSql_2.length()-1);
    		String sql = insertSql_1+insertSql_2+")";
    		log.info( "执行插入：" + sql );
    		PreparedStatement cmd = conn4write.prepareStatement(sql);
    		for( int m=0;m<records.size();m++ ){
    			ArrayList<String> oneRecord = records.get(m);
    			for( int n=0;n<colType.size();n++ ){
    				String temp = oneRecord.get(n);
    				if( temp==null || temp.equalsIgnoreCase("null") ){
						cmd.setString(n+1, null);
					}else{
	    				int type = colType.get(n).intValue();
	    				switch(type){
	    					case 2://int
	    						cmd.setInt(n+1, Integer.parseInt(temp) );
	    						break;
	    					case 4://bigint
	    						cmd.setLong(n+1, Long.parseLong(temp) );
	    						break;
	    					default://字符串或时间类型
	    						cmd.setString(n+1, temp);
	    						break;
	    				}
					}
    			}
    			cmd.addBatch();    			
    		}
    		cmd.executeBatch();			
			conn4write.commit();
			log.info( "已插入" + records.size() + "行记录。" );
			cmd.clearBatch();
			cmd.close();
    	} catch(SQLException e){
    		log.error(e.getMessage());
    		TestMain.endWhile();
    	} catch (Exception e) {
			log.error( e.getMessage() );
			e.printStackTrace();
			TestMain.endWhile();
		}
    }

	public boolean destroy(){
		boolean result = true;
		if( null!=conn4read ){
			try {
				conn4read.close();
			} catch (SQLException e) {
				log.error(e.getMessage());
				result = false;
			}
		}
		if( null!=conn4write ){
			try {
				conn4write.close();
			} catch (SQLException e) {
				log.error(e.getMessage());
				result = false;
			}
		}
		return result;
	}
	
	public void deleteData(int dbType, String table){
		String sql = "delete from " + table;
		if(dbType==1){
			try{
				conn4read.setAutoCommit(false);
				Statement stmt = conn4read.createStatement();
				stmt.executeUpdate(sql);
				conn4read.commit();
				stmt.close();
			}catch(SQLException e){
				log.error(e.getMessage());
			}
		}else{
			try{
				conn4write.setAutoCommit(false);
				Statement stmt = conn4write.createStatement();
				stmt.executeUpdate(sql);
				conn4write.commit();
				stmt.close();
			}catch(SQLException e){
				log.error(e.getMessage());
			}
		}
	}
	
}
