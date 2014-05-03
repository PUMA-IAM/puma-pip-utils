package puma.piputils;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class EntityDatabase {
	
	private static EntityDatabase instance;
	
	public static EntityDatabase getInstance() {
		if(instance == null) {
			instance = new EntityDatabase();
		}
		return instance;
	}

	/**************************
	 * CONSTRUCTOR
	 */

	private static final Logger logger = Logger.getLogger(EntityDatabase.class
			.getName());
	private static final String DEFAULT_CONNECTION = "jdbc:mysql://localhost:3306/puma-mgmt";
	private static final String DB_USER = "root";
	private static final String DB_PASSWORD = "root";
	private static String DB_CONNECTION;

	
	private static ComboPooledDataSource cpds;
	/**
	 * Initializes this new EntityDatabase. Does not open a connection yet.
	 */
	private EntityDatabase() {
		// Set the connection
		if (System.getProperty("puma.attr.db.connection") == null)
			DB_CONNECTION = DEFAULT_CONNECTION;
		else
			DB_CONNECTION = System.getProperty("puma.attr.db.connection");
		logger.info("Connection set at [" + DB_CONNECTION + "]. Initializing pool of connections...");
		try {
			cpds = new ComboPooledDataSource();
			cpds.setDriverClass( "com.mysql.jdbc.Driver" );
			cpds.setJdbcUrl( DB_CONNECTION );			
			cpds.setMaxPoolSize(30);
			cpds.setMinPoolSize(1);
			cpds.setUser(DB_USER);
			cpds.setPassword(DB_PASSWORD);
		} catch (NullPointerException e) {
			logger.log(Level.SEVERE, "Cannot open connection.", e);
		} catch (PropertyVetoException e) {
			logger.log(Level.SEVERE, "Cannot open connection.", e);
		}
	}

	/**************************
	 * DATABASE OPERATIONS
	 */
	
	private Connection conn = null;
	
	/**
	 * Sets up the connection to the database in read/write mode. 
	 * Autocommit is disabled for this connection, so know you have to commit yourself!
	 */
	public void open() {
		open(false);
	}
	
	/**
	 * Sets up the connection to the database in given mode. 
	 * Autocommit is disabled for this connection, so know you have to commit yourself!
	 */
	public void open(boolean readOnly) {
		try {
			conn = cpds.getConnection();
			conn.setReadOnly(readOnly);
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Cannot open connection.", e);
		}
	}
	
	/**
	 * Commits all operations.
	 */
	public void commit() {
		try {
			conn.commit();
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Cannot commit.", e);
		}
	}
	
	/**
	 * Closes the connection to the database.
	 */
	public void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Error when closing connection to the database.", e);
		}
	}
	
	/**
	 * Fetches all supported XACML attribute ids from the database.
	 */
	public Set<String> getSupportedXACMLAttributeIds() {
		Set<String> result = new HashSet<String>();
		try {
			String query = "SELECT xacmlIdentifier FROM SP_ATTRTYPE";
			PreparedStatement stmt;
				stmt = this.conn.prepareStatement(query);
			
			ResultSet queryResult = stmt.executeQuery();
			while (queryResult.next())
				result.add(queryResult.getString("xacmlIdentifier"));
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Could not fetch xacml attribute identifiers", e);
		}
		return result;
	}
	
	/**
	 * Fetches the data type of the attribute family with given id from the database.
	 * @return the datatype for the specified id. If multiple entities were found for the given id, then a random entity is chosen and has its datatype returned. If no id was found in the database, then String is returned.
	 */
	public DataType getDataType(String attributeId) {
		// QUESTION Jasper @ Maarten: Met attributeId wordt xacml identifier bedoeld neem ik aan? 
		List<String> result = new ArrayList<String>();
		try {
			String query = "SELECT dataType FROM SP_ATTRTYPE WHERE xacmlIdentifier=?";
			PreparedStatement stmt;
				stmt = this.conn.prepareStatement(query);
			stmt.setString(1, attributeId);
			ResultSet queryResult = stmt.executeQuery();
			while (queryResult.next())
				result.add(queryResult.getString("dataType"));
			if (!result.isEmpty()) {
				if (result.size() > 1)
					logger.warning("Multiple xacml identifiers: " + attributeId + ". Fetching random family.");
				String type = result.get(0);
				return DataType.valueOf(type);
			}
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Could not fetch xacml attribute identifiers", e);
		} catch (IllegalArgumentException e) {
			logger.log(Level.SEVERE, "Illegal datatype found in database", e);
		}
		return DataType.String;	// QUESTION Jasper @ Maarten: wat moet ik eigenlijk in dit geval het beste doen? Hiervoor ken ik de SunXACML internals nog niet goed genoeg...
	}
	
	private Long getFamilyId(String xacmlIdentifier) {
		List<Long> result = new ArrayList<Long>();
		try {
			String query = "SELECT id FROM SP_ATTRTYPE WHERE xacmlIdentifier=?";
			PreparedStatement stmt;
				stmt = this.conn.prepareStatement(query);
			stmt.setString(1, xacmlIdentifier);
			ResultSet queryResult = stmt.executeQuery();
			while (queryResult.next())
				result.add(queryResult.getLong("id"));
			if (!result.isEmpty()) {
				if (result.size() > 1)
					logger.warning("Multiple xacml identifiers: " + xacmlIdentifier + ". Fetching random family and hoping for the best.");
				Long id = result.get(0);
				return id;
			}
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Could not fetch xacml attribute identifiers", e);
		} catch (NumberFormatException e) {
			logger.log(Level.SEVERE, "Illegal family id found in database", e);
		}
		return null;
	}

	/**
	 * Fetches a string attribute from the database using the connection of this database.
	 * Does NOT commit or close.
	 */
	public Set<String> getStringAttribute(String entityId, String key) {
		// QUESTION: Jasper @ Maarten: ik neem wederom aan dat het hier gaat om de xacmlIdentifier als er een key wordt doorgegeven?
		try {
			Long familyId = getFamilyId(key);
			logger.info("Fetching attribute with family id [" + familyId + "] and user id [" + entityId + "]...");
			String query = "SELECT value FROM SP_ATTR WHERE family_id=? AND user_id=?";
			PreparedStatement stmt;
				stmt = this.conn.prepareStatement(query);
			stmt.setLong(1, familyId);
			stmt.setLong(2, Long.valueOf(entityId));
			ResultSet queryResult = stmt.executeQuery();
			
			// process the result
			Set<String> r = new HashSet<String>();
			String next;
			while (queryResult.next()) {
				next = queryResult.getString("value");
				r.add(next);
			}
			return r;
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Cannot execute query.", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Fetches an integer attribute from the database using the connection of this database.
	 * Does NOT commit or close.
	 */
	public Set<Integer> getIntegerAttribute(String entityId, String key) {
		Set<String> strings = getStringAttribute(entityId, key);
		Set<Integer> result = new HashSet<Integer>();
		for(String s: strings) {
			result.add(Integer.valueOf(s));
		}
		return result;
	}

	/**
	 * Fetches a boolean attribute from the database using the connection of this database.
	 * Does NOT commit or close.
	 */
	public Set<Boolean> getBooleanAttribute(String entityId, String key) {
		Set<String> strings = getStringAttribute(entityId, key);
		Set<Boolean> result = new HashSet<Boolean>();
		for(String s: strings) {
			if(s.equals("true")) {
				result.add(true);
			} else {
				result.add(false);
			}
		}
		return result;
	}

	/**
	 * Fetches a boolean attribute from the database using the connection of this database.
	 * Does NOT commit or close.
	 */
	public Set<Date> getDateAttribute(String entityId, String key) {
		Set<String> strings = getStringAttribute(entityId, key);
		Set<Date> result = new HashSet<Date>();
		DateFormat df = DateFormat.getInstance();
		for(String s: strings) {
			try {
				Date d = df.parse(s);
				result.add(d);
			} catch (ParseException e) {
				logger.log(Level.WARNING, "error when parsin date", e);
			}
		}
		return result;
	}

}