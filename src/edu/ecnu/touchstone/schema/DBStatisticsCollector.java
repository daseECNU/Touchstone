package edu.ecnu.touchstone.schema;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import edu.ecnu.touchstone.datatype.TSBool;
import edu.ecnu.touchstone.datatype.TSDate;
import edu.ecnu.touchstone.datatype.TSDateTime;
import edu.ecnu.touchstone.datatype.TSDecimal;
import edu.ecnu.touchstone.datatype.TSInteger;
import edu.ecnu.touchstone.datatype.TSReal;
import edu.ecnu.touchstone.datatype.TSVarchar;
import edu.ecnu.touchstone.run.DBConnector;

public class DBStatisticsCollector {

	// target database (original database)
	private String ip = null;
	private String port = null;
	private String dbName = null;
	private String userName = null;
	private String passwd = null;

	private List<Table> tables = null;

	private Logger logger = Logger.getLogger(DBStatisticsCollector.class);

	public DBStatisticsCollector(String ip, String port, String dbName, String userName, String passwd,
			List<Table> tables) {
		super();
		this.ip = ip;
		this.port = port;
		this.dbName = dbName;
		this.userName = userName;
		this.passwd = passwd;
		this.tables = tables;
	}

	@SuppressWarnings("resource")
	public void run() {
		Connection conn = DBConnector.getDBConnection(ip, port, dbName, userName, passwd);
		try {
			Statement stmt = conn.createStatement();
			for (int i = 0; i < tables.size(); i++) {
				Table table = tables.get(i);

				List<Attribute> attributes = table.getAttributes();
				for (int j = 0; j < attributes.size(); j++) {
					Attribute attribute = attributes.get(j);

					ResultSet rs = stmt.executeQuery("select count(*) from " + table.getTableName() + 
							" where " + attribute.getAttrName() + " is null");
					rs.next();
					float nullRatio = (float)rs.getLong(1) / table.getTableSize();

					rs = stmt.executeQuery("select count(distinct(" + attribute.getAttrName() + ")) from " + 
							table.getTableName() + " where " + attribute.getAttrName() + " is not null");
					rs.next();
					long cardinality = rs.getLong(1);

					switch (attribute.getDataType()) {
					case "integer":
						rs = stmt.executeQuery("select min(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						long minValue = rs.getLong(1);
						rs = stmt.executeQuery("select max(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						long maxValue = rs.getLong(1);

						attribute.setDataTypeInfo(new TSInteger(nullRatio, cardinality, minValue, maxValue));
						System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
								+ cardinality + ";" + minValue + ";" + maxValue + "]");
						break;
					case "real":
					case "decimal":
						rs = stmt.executeQuery("select min(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						double minValue2 = rs.getDouble(1);
						rs = stmt.executeQuery("select max(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						double maxValue2 = rs.getDouble(1);

						if (attribute.getDataType().equals("real")) {
							attribute.setDataTypeInfo(new TSReal(nullRatio, minValue2, maxValue2));
						} else {
							attribute.setDataTypeInfo(new TSDecimal(nullRatio, minValue2, maxValue2));
						}
						System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
								+ minValue2 + ";" + maxValue2 + "]");
						break;
					case "date":
					case "datetime":
						rs = stmt.executeQuery("select min(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						Date minDate = rs.getDate(1);
						rs = stmt.executeQuery("select max(" + attribute.getAttrName() + ") from " + table.getTableName());
						rs.next();
						Date maxDate = rs.getDate(1);

						if (attribute.getDataType().equals("date")) {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
							attribute.setDataTypeInfo(new TSDate(nullRatio, sdf.format(minDate), sdf.format(maxDate)));
							System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
									+ sdf.format(minDate) + ";" + sdf.format(maxDate) + "]");
						} else {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
							attribute.setDataTypeInfo(new TSDateTime(nullRatio, sdf.format(minDate), sdf.format(maxDate)));
							System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
									+ sdf.format(minDate) + ";" + sdf.format(maxDate) + "]");
						}
						break;
					case "varchar":
						rs = stmt.executeQuery("select avg(length(" + attribute.getAttrName() + ")) from " + table.getTableName());
						rs.next();
						float avgLength = rs.getFloat(1);
						rs = stmt.executeQuery("select max(length(" + attribute.getAttrName() + ")) from " + table.getTableName());
						rs.next();
						int maxLength = rs.getInt(1);

						attribute.setDataTypeInfo(new TSVarchar(nullRatio, avgLength, maxLength));
						System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
								+ avgLength + ";" + maxLength + "]");
						break;
					case "bool":
						rs = stmt.executeQuery("select count(*) from " + table.getTableName() + " where " + 
								attribute.getAttrName() + " is True");
						rs.next();
						float trueRatio = rs.getLong(1) / ((1 - nullRatio) * table.getTableSize());

						attribute.setDataTypeInfo(new TSBool(nullRatio, trueRatio));
						System.out.println("D[" + table.getTableName() + "." + attribute.getAttrName() + ";" + nullRatio + ";" 
								+ trueRatio + "]");
						break;
					}

					rs.close();
				} // for columns
			} // for tables
		} catch (SQLException e) {
			e.printStackTrace();
		}
		logger.info("All table information after filling the data characteristics:" + tables);
	}

	public List<Table> getTables() {
		return tables;
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(".//test//lib//log4j.properties");
		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read(".//test//input//ssb_schema_sf_1.txt");

		String ip = "10.11.1.193", port = "13306", dbName = "ssb", userName = "root", passwd = "root";
		DBStatisticsCollector collector = new DBStatisticsCollector(ip, port, dbName, userName, passwd, tables);
		collector.run();
	}
}
