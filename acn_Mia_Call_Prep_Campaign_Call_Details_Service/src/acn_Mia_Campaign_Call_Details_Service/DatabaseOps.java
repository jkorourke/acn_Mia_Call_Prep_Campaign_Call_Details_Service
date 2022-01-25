package acn_Mia_Campaign_Call_Details_Service;



import org.eclipse.rdf4j.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;

public class DatabaseOps {
	private static final Logger logger = LoggerFactory.getLogger(DatabaseOps.class);
	public static AGRepositoryConnection agOpenConn(String serverUrl, String catalog, String database, String login, String password) {

		logger.debug("Getting DB connection");
		AGRepositoryConnection conn = null; 
		try {
			conn = AGServer.createRepositoryConnection(database, catalog, serverUrl, login, password);
	} catch (RepositoryException e) {
		logger.debug("Couldn't get a connection to the AG database with:") ;
		logger.debug("REPOSITORY_ID=" + database + "\nCATALOG_ID=" + catalog + "\nSERVER_URL=" + serverUrl +
				"\nUSERNAME=" + login + "\nPASSWORD=" + password +"\n\n");
		e.printStackTrace();
	}
		logger.debug("Connection Acquired!");
		return conn;
		}

public static void agCloseConn(AGRepositoryConnection conn) {
	try {
		conn.close();
		} catch (RepositoryException e) {
			logger.debug("Failed to close connection properly");
			e.printStackTrace();			
	}
}


}
