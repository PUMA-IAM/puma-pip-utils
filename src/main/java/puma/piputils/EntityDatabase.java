/*******************************************************************************
 * Copyright 2014 KU Leuven Research and Developement - iMinds - Distrinet 
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 *    Administrative Contact: dnet-project-office@cs.kuleuven.be
 *    Technical Contact: maarten.decat@cs.kuleuven.be
 *    Author: maarten.decat@cs.kuleuven.be
 ******************************************************************************/
package puma.piputils;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class EntityDatabase {

	private static EntityDatabase instance;

	public static EntityDatabase getInstance() {
		if (instance == null) {
			instance = new EntityDatabase();
		}
		return instance;
	}

	/**************************
	 * CONSTRUCTOR
	 */

	private static final Logger logger = Logger.getLogger(EntityDatabase.class
			.getName());
	private static final String DB_USER = "root";
	private static final String DB_PASSWORD = "root";
	private static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/puma-mgmt";
	private static final String CONNECTION_ID = "puma.attr.db.connection";

	private static ComboPooledDataSource cpds;

	/**
	 * Initializes this new EntityDatabase. Does not open a connection yet.
	 */
	private EntityDatabase() {
		try {
			cpds = new ComboPooledDataSource();
			cpds.setDriverClass("com.mysql.jdbc.Driver");
			if (System.getProperty(CONNECTION_ID) == null)
				cpds.setJdbcUrl(DB_CONNECTION);
			else
				cpds.setJdbcUrl(System.getProperty(CONNECTION_ID));
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

	private PreparedStatement getStringAttributeStmt = null;
	private PreparedStatement getSupportedXACMLAttributeIdsStmt = null;

	/**
	 * Sets up the connection to the database in read/write mode. Autocommit is
	 * disabled for this connection, so know you have to commit yourself!
	 */
	public void open() {
		open(false);
	}

	/**
	 * Sets up the connection to the database in given mode. Autocommit is
	 * disabled for this connection, so know you have to commit yourself!
	 */
	public void open(boolean readOnly) {
		try {
			conn = cpds.getConnection();
			conn.setReadOnly(readOnly);
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			getStringAttributeStmt = this.conn.prepareStatement("SELECT SP_ATTR.value, SP_ATTRTYPE.dataType FROM SP_ATTRTYPE USE INDEX (familyById) INNER JOIN SP_ATTR ON SP_ATTR.family_id=SP_ATTRTYPE.id AND SP_ATTRTYPE.xacmlIdentifier=? and SP_ATTR.user_id=?");
			getSupportedXACMLAttributeIdsStmt = this.conn.prepareStatement("SELECT xacmlIdentifier FROM SP_ATTRTYPE");
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
			if (getStringAttributeStmt != null) {
				getStringAttributeStmt.close();
			}
			if (getSupportedXACMLAttributeIdsStmt != null) {
				getSupportedXACMLAttributeIdsStmt.close();
			}
			conn.close();
		} catch (SQLException e) {
			logger.log(Level.SEVERE,
					"Error when closing connection to the database.", e);
		}
	}

	/**
	 * Fetches all supported XACML attribute ids from the database.
	 */
	public Set<String> getSupportedXACMLAttributeIds() {
		Set<String> result = new HashSet<String>();
		ResultSet queryResult = null;
		try {
			queryResult = getSupportedXACMLAttributeIdsStmt.executeQuery();
			while (queryResult.next()) {
				result.add(queryResult.getString("xacmlIdentifier"));
			}
		} catch (SQLException e) {
			logger.log(Level.SEVERE,
					"Could not fetch xacml attribute identifiers", e);
		} finally {
			if(queryResult != null) {
				try {
					queryResult.close();
				} catch (SQLException e) {
					// nothing to do
					e.printStackTrace();
				}
			}
		}
		return result;
	}
	
	/**
	 * Fetches a string attribute from the database using the connection of this
	 * database. Does NOT commit or close.
	 */
	public Tuple<Set<String>, DataType> getAttribute(String entityId, String key) {
		ResultSet queryResult = null;
		try {
			logger.info("Fetching attribute with family [" + key
					+ "] and user id [" + entityId + "]...");
			getStringAttributeStmt.setString(1, key);
			getStringAttributeStmt.setLong(2, Long.valueOf(entityId));
			queryResult = getStringAttributeStmt.executeQuery();

			// process the result
			Set<String> r = new HashSet<String>();
			String next, dType;
			DataType type = null;
			while (queryResult.next()) {
				next = queryResult.getString("value");
				dType = queryResult.getString("dataType");
				if (dType != null) {
					type = DataType.valueOf(dType);
				}
				r.add(next);
			}
			return new Tuple<Set<String>, DataType>(r, type);
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Cannot execute query.", e);
			throw new RuntimeException(e);
		} catch (NumberFormatException e) {
			logger.log(Level.SEVERE, "Cannot execute query: could not find value of " + key + " (entity id \'" + entityId + "\' is not parsable) - returning no value", e);
			return new Tuple<Set<String>, DataType>(new HashSet<String>(), DataType.String);
		} finally {
			if(queryResult != null) {
				try {
					queryResult.close();
				} catch (SQLException e) {
					// nothing to do
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Fetches a string attribute from the database using the connection of this
	 * database. Does NOT commit or close.
	 */
	public Set<String> getStringAttribute(String entityId, String key) {
		// QUESTION: Jasper @ Maarten: ik neem wederom aan dat het hier gaat om
		// de xacmlIdentifier als er een key wordt doorgegeven?
		ResultSet queryResult = null;
		try {
			logger.info("Fetching attribute with family [" + key
					+ "] and user id [" + entityId + "]...");
			getStringAttributeStmt.setString(1, key);
			getStringAttributeStmt.setLong(2, Long.valueOf(entityId));
			queryResult = getStringAttributeStmt.executeQuery();

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
		} finally {
			if(queryResult != null) {
				try {
					queryResult.close();
				} catch (SQLException e) {
					// nothing to do
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Fetches an integer attribute from the database using the connection of
	 * this database. Does NOT commit or close.
	 */
	public Set<Integer> getIntegerAttribute(String entityId, String key) {
		Set<String> strings = getStringAttribute(entityId, key);
		Set<Integer> result = new HashSet<Integer>();
		for (String s : strings) {
			result.add(Integer.valueOf(s));
		}
		return result;
	}

	/**
	 * Fetches a boolean attribute from the database using the connection of
	 * this database. Does NOT commit or close.
	 */
	public Set<Boolean> getBooleanAttribute(String entityId, String key) {
		Set<String> strings = getStringAttribute(entityId, key);
		Set<Boolean> result = new HashSet<Boolean>();
		for (String s : strings) {
			if (s.equals("true")) {
				result.add(true);
			} else {
				result.add(false);
			}
		}
		return result;
	}

	/**
	 * Fetches a boolean attribute from the database using the connection of
	 * this database. Does NOT commit or close.
	 */
	public Set<Date> getDateAttribute(String entityId, String key) {
		Set<String> strings = getStringAttribute(entityId, key);
		Set<Date> result = new HashSet<Date>();
		DateFormat df = DateFormat.getInstance();
		for (String s : strings) {
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
