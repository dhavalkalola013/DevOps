
/*
 ** ${CLASS:ERI_EventManagement}
 *  JPO for Integration Event Management Code
 ** Last Modified : 12-11-2019
 */

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ericsson.api.v1.services.controller.ERI_InterfaceConstants;
import com.ericsson.eventmanagement.ERI_EventManagementUtil;
import com.ericsson.eventmanagement.ERI_RequestConsumer;
import com.matrixone.apps.domain.DomainConstants;
import com.matrixone.apps.domain.DomainObject;
import com.matrixone.apps.domain.util.MapList;
import com.matrixone.apps.framework.ui.UIUtil;

import matrix.db.Context;
import matrix.util.StringList;

public class ERI_EventManagement_mxJPO extends ERI_EventManagementUtil implements Runnable {
	/**
	 * ExecutorService
	 */
	private static ExecutorService executor = Executors.newFixedThreadPool(15);

	Properties pageProperties = null;

	/**
	 * Generic Method called by trigger to produce request in memory
	 * 
	 * @param context;@param args[0] = ${PHYSICALID} // incase of create, delete and
	 *                       promote event;${ID} incase of modify event
	 * @param args[1]        = ${TYPE} ;args[2] = ${NAME} ; args[3] = ${REVISION}//
	 *                       incase of create, delete and promote event. empty
	 *                       incase of modify event
	 * @param args[4]        = create/update/delete // respective value based on
	 *                       event
	 * @return void;@throws Exception; send an email to admin notifying event
	 *         management failure
	 */
	public void requestProducer(Context context, String[] args) {
		logger.trace(ERI_InterfaceConstants.LOG_ENTRY_METHOD + " context: {}, args: {}, ", context, args);
		try {
			produceTime = DomainConstants.EMPTY_STRING + (System.nanoTime()); // Producer Time;
			logger.debug("produceTime: {} ", produceTime);

			// produceTime = args[14];
			contextUser = context.getUser();
			logger.debug("contextUser: {} ", contextUser);

			org.slf4j.MDC.put(REQUEST_TIME_STAMP_STRING, produceTime);
			org.slf4j.MDC.put(REQUEST_USER_NAME_STRING, contextUser);

			// init producer
			initProducer(context);

			if (UIUtil.isNullOrEmpty(args[1])) {
				getTypeAndID(context, args);
			}

			// build payload args
			boolean callEvent = buildPayLoadArgs(context, args);
			logger.debug("callEvent: {} ", callEvent);

			// Create new thread
			if (callEvent) {
				String isSequential = getPersonProperty(context, "isSequential");
				logger.debug("isSequential: {} ", isSequential);

				if ("Yes".equals(isSequential)) {
					payLoadJsonString = getPayLoadJSON(context, args);
					logger.debug("payLoadJsonString: {} ", payLoadJsonString);

					consumerRequest(payLoadJsonString);
				} else {
					this.payLoadJsonString = getPayLoadJSON(context, args);
					logger.debug("payLoadJsonString: {} ", payLoadJsonString);

					executor.submit(this);

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception in requestProducer:", e);
			try {
				notifyForFailure(getHTMLMsgBody(getStackTrace(e), getPayLoadJSON(context, args)));
			} catch (Exception exp) {
				logger.error("Exception in notifyForFailure:", exp);
			}
		}
	}

	/**
	 * Create arguments for payload
	 * 
	 * @param context;String array args[0] = ${PHYSICALID} //for create, delete and
	 *                       promote.${ID} for modify event; args[1] =
	 *                       ${TYPE};args[2] = ${NAME};args[3] = ${REVISION}//for
	 *                       create, delete and promote event. empty for modify;
	 *                       args[4] = create/update/delete // respective value
	 *                       based on event;args[5] = Modified date args[6] =
	 *                       Project
	 * @throws Exception; send an email to admin notifying event management failure
	 */
	public boolean buildPayLoadArgs(Context context, String[] payloadArgs) throws Exception {
		logger.trace(ERI_InterfaceConstants.LOG_ENTRY_METHOD + " context: {}, payloadArgs: {} ", context, payloadArgs);

		boolean callEvent = true;
		String event = payloadArgs[4];
		logger.debug("event: {} ", event);

		String strType = payloadArgs[1];
		logger.debug("strType: {} ", strType);

		String strConfiguredTypeForEventManagement = readPropertyValue(context, KEY_Configure_TYPES);
		logger.debug("strConfiguredTypeForEventManagement: {} ", strConfiguredTypeForEventManagement);

		if (UIUtil.isNullOrEmpty(strType) || !strConfiguredTypeForEventManagement.contains(strType)) {
			return false;
		}

		String sStopTrigger = getPersonProperty(context, ENABLE_EVENT_MANAGEMENT_STRING);
		logger.debug("sStopTrigger: {} ", sStopTrigger);

		StringList busSelect = getSelectableList(context, payloadArgs[1]);
		logger.debug("busSelect: {} ", busSelect);

		if (!sStopTrigger.equalsIgnoreCase(NO_STRING)) {
			if (CREATE_STRING.equalsIgnoreCase(event)) {
				DomainObject dObjDoc = new DomainObject(payloadArgs[0]);
				if ("Document".equalsIgnoreCase(payloadArgs[1])
						&& "Version".equalsIgnoreCase(dObjDoc.getInfo(context, DomainConstants.SELECT_POLICY))) {
					callEvent = false;
					logger.debug("callEvent: {} ", callEvent);
					return callEvent;
				}
				Map<?, ?> attMap = getObjectInformation(context, payloadArgs[0], busSelect);
				logger.debug("attMap: {} ", attMap);

				payloadArgs[5] = (String) attMap.get(DomainConstants.SELECT_MODIFIED);
				payloadArgs[6] = (String) attMap.get(DomainConstants.SELECT_PROJECT);
				if (attMap.containsKey(attr_GPI)) {
					payloadArgs[13] = (String) attMap.get(attr_GPI);
				}
			}
			if (UPDATE_STRING.equalsIgnoreCase(event)) {
				Map<?, ?> attMap = getObjectInformation(context, payloadArgs[0], busSelect);
				logger.debug("attMap: {} ", attMap);

				callEvent = checkEventsWithCreate(context, (String) attMap.get(DomainConstants.SELECT_ORIGINATED),
						(String) attMap.get(DomainConstants.SELECT_MODIFIED));
				logger.debug("callEvent: {} ", callEvent);

				payloadArgs[0] = (String) attMap.get(SELECT_PHYSICALID);
				payloadArgs[1] = (String) attMap.get(DomainConstants.SELECT_TYPE);
				payloadArgs[2] = (String) attMap.get(DomainConstants.SELECT_NAME);
				payloadArgs[3] = (String) attMap.get(DomainConstants.SELECT_REVISION);
				payloadArgs[5] = (String) attMap.get(DomainConstants.SELECT_MODIFIED);
				payloadArgs[6] = (String) attMap.get(DomainConstants.SELECT_PROJECT);
				if (attMap.containsKey(attr_GPI)) {
					payloadArgs[13] = (String) attMap.get(attr_GPI);
				}
			}
			if (DELETE_STRING.equalsIgnoreCase(event)) {
				payloadArgs[5] = com.matrixone.apps.domain.util.PropertyUtil.getRPEValue(context, SELECT_ENV_TIMESTAMP,
						false);
				payloadArgs[6] = com.matrixone.apps.domain.util.PropertyUtil.getRPEValue(context, SELECT_ENV_PROJECT,
						false);
			}
			if (CONNECT_STRING.equalsIgnoreCase(event) || DISCONNECT_STRING.equalsIgnoreCase(event)) {
				String objectList[] = new String[2];
				objectList[0] = payloadArgs[0];// from internal id
				objectList[1] = payloadArgs[7]; // to internal id

				DomainObject dObjFromType = new DomainObject(objectList[0]);
				if ("Model".equalsIgnoreCase(dObjFromType.getInfo(context, DomainConstants.SELECT_TYPE))) {

					callEvent = false;
					logger.debug("callEvent: {} ", callEvent);

					return callEvent;
				}

				MapList mlObjectInformation = getObjectsInformation(context, objectList, busSelect);
				logger.debug("mlObjectInformation: {} ", mlObjectInformation);

				String prodModifiedDate = getObjectAttributeValue(context, payloadArgs[0],
						DomainConstants.SELECT_MODIFIED, mlObjectInformation);
				logger.debug("prodModifiedDate: {} ", prodModifiedDate);

				String subProdModifiedDate = getObjectAttributeValue(context, payloadArgs[7],
						DomainConstants.SELECT_MODIFIED, mlObjectInformation);
				logger.debug("subProdModifiedDate: {} ", subProdModifiedDate);

				payloadArgs[5] = prodModifiedDate;
				payloadArgs[12] = subProdModifiedDate;
				payloadArgs[8] = getTypeDisplayName(context, payloadArgs[8]);
				payloadArgs[13] = getObjectAttributeValue(context, payloadArgs[0], attr_GPI, mlObjectInformation);
				payloadArgs[14] = getObjectAttributeValue(context, payloadArgs[7], attr_GPI, mlObjectInformation);

			}
			if (REVISION_STRING.equalsIgnoreCase(event)) {
				DomainObject domObj = new DomainObject(payloadArgs[0]);
				String physicaId = domObj.getInfo(context, SELECT_PHYSICALID);
				logger.debug("physicaId: {} ", physicaId);

				payloadArgs[0] = physicaId;
				Map<?, ?> attMap = getObjectInformation(context, payloadArgs[0], busSelect);
				logger.debug("attMap: {} ", attMap);

				payloadArgs[5] = (String) attMap.get(DomainConstants.SELECT_MODIFIED);
				payloadArgs[6] = (String) attMap.get(DomainConstants.SELECT_PROJECT);
				payloadArgs[4] = "create";
				if (attMap.containsKey(attr_GPI)) {
					payloadArgs[13] = (String) attMap.get(attr_GPI);
				}

			}

			payloadArgs[1] = getTypeDisplayName(context, payloadArgs[1]);

			fType = payloadArgs[1];
			fName = payloadArgs[2];
			fRev = payloadArgs[3];
			event = payloadArgs[4];
			tType = payloadArgs[8];
			tName = payloadArgs[9];
			tRev = payloadArgs[10];
			logEvent = event;

		} else {
			callEvent = false;
			logger.debug("callEvent: {} ", callEvent);
		}
		logger.debug("payloadArgs: {} ", payloadArgs);

		logger.trace(ERI_InterfaceConstants.LOG_EXIT_METHOD + " callEvent : {}", callEvent);
		return callEvent;
	}

	/**
	 * run method
	 * 
	 * @return Map
	 */
	public void run() {
		consumerRequest(this.payLoadJsonString);
	}

	/**
	 * Generic Method to consume the request planced in memory by worker by sending
	 * it to DIL
	 * 
	 * @param context;args[0] = ${PHYSICALID} //for create, delete and promote.${ID}
	 *                        for of modify event
	 * @param args[1]=        ${TYPE};args[2]=${NAME};args[3]=${REVISION}//for
	 *                        create, delete and promote.empty for modify
	 * @param args[4]=        create/update/delete //event name ;args[5]=modified
	 *                        date;args[6]=Collaboration Space;args[14] = producer
	 *                        TimeStamp in neno second
	 * @return void
	 * @throws Exception; send an email to admin notifying event management failure
	 */
	public void consumerRequest(String palLoadJsonString) {
		logger.trace(ERI_InterfaceConstants.LOG_ENTRY_METHOD + " palLoadJsonString: {}", palLoadJsonString);

		String result = "";
		org.slf4j.MDC.put(REQUEST_TIME_STAMP_STRING, produceTime);
		org.slf4j.MDC.put(REQUEST_USER_NAME_STRING, contextUser);
		org.slf4j.MDC.put(TYPE_STRING, fType);
		org.slf4j.MDC.put(NAME_STRING, fName);
		org.slf4j.MDC.put(REVISION_STRING, fRev);
		org.slf4j.MDC.put(TO_TYPE_STRING, tType);
		org.slf4j.MDC.put(TO_NAME_STRING, tName);
		org.slf4j.MDC.put(TO_REVISION_STRING, tRev);
		org.slf4j.MDC.put(EVENT_STRING, logEvent);

		try {

			Long startTime = Long.valueOf(produceTime);
			logger.debug("startTime: {} ", startTime);

			ERI_RequestConsumer reqConsumer = new ERI_RequestConsumer(timeout, clientUrl, strCredentials);
			int retryIndex = 0;
			for (retryIndex = 0; (retryIndex < retryCount
					&& ("".equals(result) || result.contains("timeout"))); retryIndex++) {
				result = reqConsumer.consumerRequest(palLoadJsonString);
				logger.debug("result: {} ", result);

				Thread.sleep((int) (retryInterval * 1000));
			}

			Long endTime = System.nanoTime();
			logger.debug("endTime: {} ", endTime);

			Long timeTaken = (endTime - startTime) / 1_000_000;
			logger.debug("timeTaken: {} ", timeTaken);

			xTrackerLogger.info("Success");
			boolean response = checkResponse(result);
			logger.debug("response: {} ", response);

			if (!response) {
				notifyForFailure(getHTMLMsgBody(result, palLoadJsonString));
			}
		} catch (Exception exp) {
			exp.printStackTrace();
			logger.error("Exception in consumerRequest:", exp);
			xTrackerLogger.info("Failed," + exp.toString());
		}
		logger.trace(ERI_InterfaceConstants.LOG_EXIT_METHOD);

	}

	public void getTypeAndID(Context context, String[] args) throws Exception {
		logger.trace(ERI_InterfaceConstants.LOG_ENTRY_METHOD + " context: {}, args: {}", context, args);
		String objID = args[0];
		if (!UIUtil.isNullOrEmpty(objID)) {
			DomainObject dom = new DomainObject(objID);
			args[0] = dom.getInfo(context, SELECT_PHYSICALID);
			args[1] = dom.getInfo(context, DomainObject.SELECT_TYPE);
		}
		logger.trace(ERI_InterfaceConstants.LOG_EXIT_METHOD);
	}

}