package acn_Mia_Campaign_Call_Details_Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Date;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;

/**
 * @author jor
 *
 * 
 */
public class N3utils {
	private static final Logger logger = LoggerFactory.getLogger(N3utils.class);

	/**
	 * @param args
	 */
	private static final boolean STANDARD_REPORT = false;

	public static void main(String[] args) {

		int dateDiff;
		// dateDiff = calcDateDiff();
	}

	public static int calcDateDiff(String agServerUrl, String agRepositoryId, String agCatalogId, String agUserName, String agPassword, String newMssalesGraph, String sqlServerMssalesHostString,
			String sqlserverMssalesDatabase, String sqlserverMssalesLogin, String sqlserverMssalesPassword, String msSalesSqlLogQueryString) {
		int dateDiff = -9999; // Initialized to impossible calculated value.

		LocalDate maxSaleInGraph;
		LocalDate maxSaleInEdw;

		AGRepositoryConnection agConnection = connectDb(agServerUrl, agRepositoryId, agCatalogId, agUserName, agPassword);

		maxSaleInGraph = retrieveLastestSaleDate(agConnection, newMssalesGraph);
		logger.debug("String Representation of MaxSaleInGraph LocalDate: " + maxSaleInGraph);

		closeDb(agConnection);

		Connection sqlServerConnection = openSqlServerConn(sqlServerMssalesHostString, sqlserverMssalesDatabase, sqlserverMssalesLogin, sqlserverMssalesPassword);

		maxSaleInEdw = retrieveLatestDWSaleDate(sqlServerConnection, msSalesSqlLogQueryString);

		closeSqlServerConn(sqlServerConnection);
		dateDiff = (int) ChronoUnit.DAYS.between(maxSaleInGraph, maxSaleInEdw);
		logger.debug("dateDiff: " + dateDiff);

		return dateDiff;
	}

	private static LocalDate retrieveLatestDWSaleDate(Connection sqlServerConnection, String msSalesSqlLogQueryString) {
		String LatestSaleDateEdwString = "not_initialized";
		LocalDate LatestSaleDateEdw;
		Statement statement = null;
		ResultSet resultSet = null;

		try {
			statement = sqlServerConnection.createStatement();
			resultSet = statement.executeQuery(msSalesSqlLogQueryString);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		try {
			while (resultSet.next()) {
				LatestSaleDateEdwString = resultSet.getString("DATE");
			}
			logger.debug("LatestSaleDateEdwString: " + LatestSaleDateEdwString);
			resultSet.close();
		} catch (SQLException e) {
			logger.debug("");
			e.printStackTrace();
		}
		LatestSaleDateEdw = LocalDate.parse(LatestSaleDateEdwString);

		return LatestSaleDateEdw;
	}

	public static AGRepositoryConnection connectDb(String serverUrl, String dbName, String catalogName, String login, String password) {
		// Get repository connection
		logger.debug("Getting DB connection");
		AGRepositoryConnection conn = null;
		try {
			conn = AGServer.createRepositoryConnection(dbName, catalogName, serverUrl, login, password);
			// conn.setAutoCommit(true);
			// conn.rollback();
			// conn.rollback();
			// conn.commit();
		} catch (RepositoryException e) {
			logger.debug("Couldn't get a connection to the AG database with:");
			logger.debug("REPOSITORY_ID=" + dbName + "\nCATALOG_ID=" + catalogName + "\nSERVER_URL=" + serverUrl + "\nUSERNAME=" + login + "\nPASSWORD=" + password + "\n\n");
			e.printStackTrace();
		}
		if (!STANDARD_REPORT) {
			logger.debug("Connection Acquired!");
		}
		return conn;
	}

	public static void closeDb(AGRepositoryConnection conn) {
		try {
			conn.close();
		} catch (RepositoryException e) {
			logger.debug("Failed to close connection properly");
			e.printStackTrace();
		}
		logger.debug("Closed Connection");
	}

	private static LocalDate retrieveLastestSaleDate(AGRepositoryConnection conn, String newMsSalesGraph) {
		String maxSaleInGraphString = "not_set";
		LocalDate maxSaleInGraph = null;

		String queryString = "select distinct (max(?date) as ?MaxDate) { " + "graph <http://n3results.com/crm/NewMsSalesIngested>" + " { ?s <http://n3results.com/crm/pred#MsSalesTrxDate> ?date } }";

		logger.debug("Retrieve Max Sales Date from Graph");

		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
		TupleQueryResult result;

		logger.debug("Running Query: " + queryString);

		try {
			result = tupleQuery.evaluate();

			while (result.hasNext()) {
				BindingSet bindingSet = result.next();
				maxSaleInGraphString = bindingSet.getBinding("MaxDate").toString().replaceAll("(^.*=\")|(\"\\^\\^.*$)", "");
			} // end while
			result.close();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		logger.debug("MaxSaleInGraphString: " + maxSaleInGraphString);
		maxSaleInGraph = LocalDate.parse(maxSaleInGraphString);
		logger.debug("String Representation of MaxSaleInGraph LocalDate: " + maxSaleInGraph);
		return maxSaleInGraph;
	}

	public static Connection openSqlServerConn(String sqlServerCurationHostString, String sqlServerMsSalesDatabase, String sqlServerCurationLogin, String sqlServerCurationPassword) {
		Connection conn = null;
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			conn = DriverManager.getConnection("jdbc:sqlserver://" + sqlServerCurationHostString + ";user=" + sqlServerCurationLogin + ";password=" + sqlServerCurationPassword + ";DatabaseName="
					+ sqlServerMsSalesDatabase + ";");
			logger.debug("SQL Server Database: " + sqlServerMsSalesDatabase);
			logger.debug("SQL Server Connection Acquired");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static void closeSqlServerConn(Connection sqlServerConnection) {
		try {
			sqlServerConnection.close();
			logger.debug("SQL Server Connection Closed");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static String elapsedTime(Instant start) {
		Instant now;
		now = Instant.now();
		String elapsedTimeString = "Elapsed Time: " + ChronoUnit.MILLIS.between(start, now) + " ms";
		return elapsedTimeString;
	}

	public static String elapsedTimeMillis(Instant start) {
		Instant now;
		now = Instant.now();
		String elapsedTimeString = String.valueOf(ChronoUnit.MILLIS.between(start, now));
		return elapsedTimeString;
	}

	public static void printStringArray(String[] extCommandStringArray) {
		for (int i = 0; i < extCommandStringArray.length; i++) {
			System.out.print(extCommandStringArray[i] + " ");
		}
		logger.debug("/n");
	}

	public static String callExtCommand(Instant start, String[] externalCommandString) {
		StringBuffer output = new StringBuffer();
		Process extCommand;
		try {
			extCommand = Runtime.getRuntime().exec(externalCommandString);
			extCommand.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(extCommand.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
				logger.debug(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.print("Finished External Command: ");
		N3utils.elapsedTime(start);
		return output.toString();
	}

	public static String dateTimeStringForLabel(Instant instant) {
		Date date = Date.from(instant);
		DateFormat dateFormat = new SimpleDateFormat("YYMMdd-HHmmss");
		String dateString = dateFormat.format(date);
		// logger.debug(dateString);
		return dateString;
	}

	public static String date(Instant instant) {
		Date date = Date.from(instant);
		DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
		String dateString = dateFormat.format(date);
		return dateString;
	}

	public static String dateTime(Instant instant) {
		Date date = Date.from(instant);
		DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSS'Z'");
		String dateString = dateFormat.format(date);
		return dateString;
	}

	public static String dayOfWeekFromDate(String date) {
		SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd");
		Date myDate = null;
		try {
			myDate = sdf.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		sdf.applyPattern("EEE");
		String dayOfWeek = sdf.format(myDate);
		return dayOfWeek;
	}

	public static void dropGraph(AGRepositoryConnection graphConn, String graph) {
		logger.debug("Begin: Drop graph <" + graph + ">");
		graphConn.begin();
		StringBuilder dropGraphStringBuilder = new StringBuilder()
				.append("PREFIX franzOption_memoryLimit: <franz:150G> \n" + "PREFIX franzOption_defaultDatasetBehavior: <franz:rdf> \n" + "drop graph  <" + graph + "> ");

		Update updateDropGraph = graphConn.prepareUpdate(dropGraphStringBuilder.toString());

		updateDropGraph.execute();

		logger.debug("Complete: drop graph <" + graph + ">");
		graphConn.commit();
	}

	static void updateGraph(AGRepositoryConnection graphConn, String updateActiveAccountsGraphString) {
		logger.debug("Begin Update: \n" + updateActiveAccountsGraphString.toString());

		Update sparqlUpdate = graphConn.prepareUpdate(updateActiveAccountsGraphString.toString());

//		graphConn.begin();
		sparqlUpdate.execute();
//		graphConn.commit();

		logger.debug("Complete: Sparql Update");

	}

	public static void agraphOptimize(AGRepositoryConnection queryGraphConn) {
		logger.debug("Starting Database Optimization: " + queryGraphConn.getRepository());
		queryGraphConn.optimizeIndices(true);

		logger.debug("Completed Database Optimization: " + queryGraphConn.getServer());

	}

	/**
	 * Tests for valid JSON before trying to create a JSON object with a string.
	 * 
	 * @param test
	 * @return
	 */
	public static boolean isJsonValid(String test) {
		try {
			new JSONObject(test);
		} catch (JSONException ex) {
			try {
				new JSONArray(test);
			} catch (JSONException ex1) {
				return false;
			}
		}
		return true;
	}

	public static boolean hasJsonObject(JSONObject json, String key) {
		if (json.get(key) instanceof JSONObject) {
			// Yes, it contains at least one JSONObject, whose key is `key`
			return true;
		} else {
			return false;
		}
	}

	public static boolean hasJsonArray(JSONObject json, String key) {
		if (json.get(key) instanceof JSONArray) {
			// Yes, it contains at least one JSONObject, whose key is `key`
			return true;
		} else {
			return false;

		}

	}

}