/*******************************************************************************
 * Copyright 2013 KU Leuven Research and Developement - IBBT - Distrinet 
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

import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.logging.Logger;

import puma.util.timing.TimerFactory;

import com.codahale.metrics.Timer;
import com.sun.xacml.EvaluationCtx;
import com.sun.xacml.attr.AttributeDesignator;
import com.sun.xacml.attr.AttributeValue;
import com.sun.xacml.attr.BagAttribute;
import com.sun.xacml.attr.BooleanAttribute;
import com.sun.xacml.attr.DateTimeAttribute;
import com.sun.xacml.attr.IntegerAttribute;
import com.sun.xacml.attr.StringAttribute;
import com.sun.xacml.cond.EvaluationResult;
import com.sun.xacml.ctx.Status;
import com.sun.xacml.finder.AttributeFinderModule;

/**
 * An attribute finder module with hard coded values.
 * 
 * @author maartend
 * 
 */
public class QueryAttributeFinderModule extends AttributeFinderModule {

	/**
	 * The logger we'll use for all messages
	 */
	private static final Logger logger = Logger
			.getLogger(QueryAttributeFinderModule.class.getName());

	/**
	 * The URI identifying the resource id element.
	 */
	public static URI resourceIdIdentifier = null;
	static {
		try {
			resourceIdIdentifier = new URI("object:id");
			// resourceIdIdentifier = new URI(
			// "urn:oasis:names:tc:xacml:1.0:resource:resource-id");
		} catch (URISyntaxException e) {
			// will not happen with this code
		}
	}

	/**
	 * The URI identifying the subject id element.
	 */
	public static URI subjectIdIdentifier = null;
	static {
		try {
			subjectIdIdentifier = new URI("subject:id");
			// subjectIdIdentifier = new URI(
			// "urn:oasis:names:tc:xacml:1.0:subject:subject-id");
		} catch (URISyntaxException e) {
			// will not happen with this code
		}
	}

	private EntityDatabase edb;
	
	private static final String TIMER_NAME = "database.fetch";

	public QueryAttributeFinderModule(EntityDatabase edb) {
		this.edb = edb;
	}

	public QueryAttributeFinderModule() {
		this(EntityDatabase.getInstance());
	}

	/**
	 * We only support designators, not selectors.
	 */
	@Override
	public boolean isDesignatorSupported() {
		return true;
	}

	/**
	 * We only support target attributes.
	 */
	@Override
	public Set<Integer> getSupportedDesignatorTypes() {
		Set<Integer> set = new HashSet<Integer>();
		set.add(AttributeDesignator.SUBJECT_TARGET);
		set.add(AttributeDesignator.RESOURCE_TARGET);
		set.add(AttributeDesignator.ENVIRONMENT_TARGET);
		return set;
	}

	/**
	 * Returns the one identifier this module supports.
	 */
	@Override
	public Set<String> getSupportedIds() {
		Set<String> set = new HashSet<String>();
		set.addAll(this.edb.getSupportedXACMLAttributeIds());
		return set;
	}

	/**
	 * Returns whether this module supports the given id.
	 */
	public boolean supportsId(String id) {
		for (String s : getSupportedIds()) {
			if (id.equals(s)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns whether this module supports the given id.
	 */
	public boolean supportsId(URI id) {
		return supportsId(id.toString());
	}

	/**
	 * Retrieve an attribute from the database.
	 * 
	 * If the attribute cannot be found (this is not an error!), returns an
	 * empty bag.
	 */
	public EvaluationResult findAttribute(URI attributeType, URI attributeId,
			URI issuer, URI subjectCategory, EvaluationCtx context,
			int designatorType) {
		// make sure we support this attribute id
		/*if (!supportsId(attributeId)) {
			return new EvaluationResult(
					BagAttribute.createEmptyBag(attributeType));
		}*/

		// // make sure we've been asked for a string
		// if (!attributeType.toString().equals(StringAttribute.identifier)) {
		// return new EvaluationResult(
		// BagAttribute.createEmptyBag(attributeType));
		// }
		// DEBUG
		logger.info("Fetching attribute " + attributeId.toASCIIString() + " ("
				+ designatorType + ")");
		// / DEBUG
		// We're OK to go, so start with fetching the
		// entity id (a lot of cruft...)
		String entityId;
		if (designatorType == AttributeDesignator.SUBJECT_TARGET) {
			// fetch the subject id
			EvaluationResult subjectIdResult = context.getSubjectAttribute(
					StringAttribute.identifierURI, subjectIdIdentifier, issuer,
					subjectCategory);
			if (subjectIdResult.indeterminate()) {
				return subjectIdResult;
			}
			// check that we succeeded in getting the subject identifier
			BagAttribute subjectIdBag = (BagAttribute) (subjectIdResult
					.getAttributeValue());
			if (subjectIdBag.isEmpty()) {
				List<String> code = new ArrayList<String>();
				code.add(Status.STATUS_MISSING_ATTRIBUTE);
				Status status = new Status(code, "missing subject-id");
				return new EvaluationResult(status);
			} else if (subjectIdBag.size() > 1) {
				List<String> code = new ArrayList<String>();
				code.add(Status.STATUS_PROCESSING_ERROR);
				Status status = new Status(code, "multiple subject ids");
				return new EvaluationResult(status);
			}
			// now get the last (and only) element in the bag
			String subjectId = null;
			for (Object o : subjectIdBag) {
				subjectId = ((StringAttribute) o).getValue();
			}
			assert (subjectId != null);
			logger.fine("Subject identifier: " + subjectId);
			entityId = subjectId;
		} else if (designatorType == AttributeDesignator.RESOURCE_TARGET) {
			// fetch the resource id (also a lot of cruft...)
			EvaluationResult resourceIdResult = context
					.getResourceAttribute(StringAttribute.identifierURI,
							resourceIdIdentifier, issuer);
			if (resourceIdResult.indeterminate()) {
				return resourceIdResult;
			}
			// check that we succeeded in getting the resource identifier
			BagAttribute resourceIdBag = (BagAttribute) (resourceIdResult
					.getAttributeValue());
			if (resourceIdBag.isEmpty()) {
				List<String> code = new ArrayList<String>();
				code.add(Status.STATUS_MISSING_ATTRIBUTE);
				Status status = new Status(code, "missing resource-id");
				return new EvaluationResult(status);
			} else if (resourceIdBag.size() > 1) {
				List<String> code = new ArrayList<String>();
				code.add(Status.STATUS_PROCESSING_ERROR);
				Status status = new Status(code, "multiple resource ids");
				return new EvaluationResult(status);
			}
			// now get the last (and only) element in the bag
			String resourceId = null;
			for (Object o : resourceIdBag) {
				resourceId = ((StringAttribute) o).getValue();
			}
			assert (resourceId != null);
			logger.fine("Resource identifier: " + resourceId);
			entityId = resourceId;
		} else if (designatorType == AttributeDesignator.ENVIRONMENT_TARGET) {
			entityId = "environment";
		} else {
			logger.warning("WTF, attibute of designatorType " + designatorType
					+ " requested from HardcodedAttributeFinderModule?");
			return new EvaluationResult(
					BagAttribute.createEmptyBag(attributeType));
		}

		// now that we have the entity id: retrieve the necessary
		// value from the database for this subject
		List<AttributeValue> values = getAttributeValue(attributeId.toString(),
				entityId);

		if (values.isEmpty()) {
			logger.info("No values received from the db for attribute #" + attributeId);
			return new EvaluationResult(
					BagAttribute.createEmptyBag(attributeType));
		} else {
			return new EvaluationResult(new BagAttribute(attributeType, values));
		}
	}
	
	/**
	 * 
	 */
	public List<AttributeValue> getAttributeValue(String attributeId,
			String entityId) {
		Timer.Context timerCtx = TimerFactory.getInstance().getTimer(getClass(), TIMER_NAME).time();
		List<AttributeValue> result = _getAttributeValue(attributeId, entityId);
		timerCtx.stop();
		return result;
	}

	/**
	 * This is the actual getAttributeValue(). It is separated in order
	 * to wrap it in timer code.
	 * 
	 * @param attributeId
	 * @param entityId
	 * @return
	 */
	public List<AttributeValue> _getAttributeValue(String attributeId,
			String entityId) {
		// FIXME if the attribute ids are not unique over the different tenants,
		// we need
		// to provide the entityId as well to first provide the organization
		// owning the attribute family --> EXTRA JOIN
		Tuple<Set<String>, DataType> queryResult = this.edb.getAttribute(entityId, attributeId);
				
		List<AttributeValue> result = new ArrayList<AttributeValue>();
		if (queryResult.hasType()) {
			if (queryResult.getType().equals(DataType.String)) {
				Collection<String> values = queryResult.getData();
				if (values.isEmpty()) {
					logger.info("No values found for attribute (attribute id: "
							+ attributeId + ", entity id: " + entityId + ")");
				} else {
					for (String s : values) {
						result.add(new StringAttribute(s));
					}
				}
			} else if (queryResult.getType().equals(DataType.Boolean)) {
				Collection<Boolean> values = new HashSet<Boolean>();
				for (String next: queryResult.getData()) {
					values.add(Boolean.parseBoolean(next));
					logger.info("Found attribute: [" + next + "]");
				}
				if (values.isEmpty()) {
					logger.info("No values found for attribute (attribute id: "
							+ attributeId + ", entity id: " + entityId + ")");
				} else {
					for (Boolean b : values) {
						result.add(BooleanAttribute.getInstance(b));
					}
				}
			} else if (queryResult.getType().equals(DataType.DateTime)) {
				Collection<Date> values = new HashSet<Date>();
				for (String next: queryResult.getData())
					try {
						values.add(new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH).parse(next)); // DEBUG LATER this may not be the format in which the date is stored in the db
					} catch (ParseException e) {
						logger.warning("Could not parse date from internal format. Returning empty result...");
					}	
				if (values.isEmpty()) {
					logger.info("No values found for attribute (attribute id: "
							+ attributeId + ", entity id: " + entityId + ")");
				} else {
					for (Date d : values) {
						result.add(new DateTimeAttribute(d));
					}
				}
			} else if (queryResult.getType().equals(DataType.Integer)) {
				if (queryResult.getData().isEmpty()) {
					logger.info("No values found for attribute (attribute id: "
							+ attributeId + ", entity id: " + entityId + ")");
				} else {
					for (String next: queryResult.getData()) {
						result.add(IntegerAttribute.getInstance(next));
					}
				}
			}
		}
		return result;
	}
}

