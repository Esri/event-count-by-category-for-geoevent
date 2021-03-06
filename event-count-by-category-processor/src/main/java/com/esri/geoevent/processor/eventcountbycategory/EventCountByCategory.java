/*
  Copyright 2017 Esri

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.​

  For additional information, contact:
  Environmental Systems Research Institute, Inc.
  Attn: Contracts Dept
  380 New York Street
  Redlands, California, USA 92373

  email: contracts@esri.com
*/

package com.esri.geoevent.processor.eventcountbycategory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import com.esri.core.geometry.MapGeometry;
import com.esri.ges.core.Uri;
import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldExpression;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.framework.i18n.BundleLogger;
import com.esri.ges.framework.i18n.BundleLoggerFactory;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;
import com.esri.ges.util.Converter;
import com.esri.ges.util.Validator;

public class EventCountByCategory extends GeoEventProcessorBase implements GeoEventProducer, EventUpdatable
{
	private static final BundleLogger							LOGGER				= BundleLoggerFactory.getLogger(EventCountByCategory.class);

	private EventCountByCategoryNotificationMode	notificationMode;
	private long																	reportInterval;

	private final Map<String, String>							trackCache		= new ConcurrentHashMap<String, String>();
	private final Map<String, Counters>						counterCache	= new ConcurrentHashMap<String, Counters>();

	private Messaging															messaging;
	private GeoEventCreator												geoEventCreator;
	private GeoEventProducer											geoEventProducer;
	private Date																	resetTime;
	private boolean																autoResetCounter;
	private boolean																clearCache;
	private Timer																	clearCacheTimer;
	private String																categoryField;
	private Uri																		definitionUri;
	private String																definitionUriString;
	private boolean																isCounting		= false;

	final Object																	lock1					= new Object();

	class Counters
	{
		private String			id;
		private Long				cumulativeCount	= 0L;
		private Long				currentCount		= 0L;
		private MapGeometry	geometry;
		private boolean			hasChanged;

		public Counters()
		{
		}

		public Long getCumulativeCount()
		{
			return cumulativeCount;
		}

		public void setCumulativeCount(Long cumulativeCount)
		{
			hasChanged = (cumulativeCount != this.cumulativeCount);
			this.cumulativeCount = cumulativeCount;
			if (hasChanged == true)
			{
				sendReport();
			}
		}

		public Long getCurrentCount()
		{
			return currentCount;
		}

		public void setBothCurrentAndCumulativeCounts(Long currentCount, Long cumulativeCount)
		{
			hasChanged = (currentCount != this.currentCount) || (cumulativeCount != this.cumulativeCount);
			this.currentCount = currentCount;
			this.cumulativeCount = cumulativeCount;
			if (hasChanged == true)
			{
				sendReport();
			}
		}

		public void setCurrentCount(Long currentCount)
		{
			hasChanged = (currentCount != this.currentCount);
			this.currentCount = currentCount;
			if (hasChanged == true)
			{
				sendReport();
			}
		}

		public MapGeometry getGeometry()
		{
			return geometry;
		}

		public void setGeometry(MapGeometry geometry)
		{
			this.geometry = geometry;
		}

		public String getId()
		{
			return id;
		}

		public void setId(String id)
		{
			this.id = id;
		}

		public void sendReport()
		{
			if (notificationMode != EventCountByCategoryNotificationMode.OnChange)
			{
				return;
			}
			try
			{
				send(createCounterGeoEvent(id, this));
			}
			catch (MessagingException error)
			{
				LOGGER.error("SEND_REPORT_ERROR", id, error.getMessage());
				LOGGER.info(error.getMessage(), error);
			}
		}
	}

	class ClearCacheTask extends TimerTask
	{
		public void run()
		{
			if (autoResetCounter == true)
			{
				for (String catId : counterCache.keySet())
				{
					Counters counters = new Counters();
					counterCache.put(catId, counters);
				}
			}
			// clear the cache
			if (clearCache == true)
			{
				counterCache.clear();
				trackCache.clear();
			}
		}
	}

	class ReportGenerator implements Runnable
	{
		private Long	reportInterval	= 5000L;

		public ReportGenerator(String category, Long reportInterval)
		{
			this.reportInterval = reportInterval;
		}

		@Override
		public void run()
		{
			while (isCounting)
			{
				try
				{
					Thread.sleep(reportInterval);
					if (notificationMode != EventCountByCategoryNotificationMode.Continuous)
					{
						continue;
					}

					for (String catId : counterCache.keySet())
					{
						Counters counters = counterCache.get(catId);
						try
						{
							send(createCounterGeoEvent(catId, counters));
						}
						catch (MessagingException error)
						{
							LOGGER.error("SEND_REPORT_ERROR", catId, error.getMessage());
							LOGGER.info(error.getMessage(), error);
						}
					}
				}
				catch (InterruptedException error)
				{
					LOGGER.error(error.getMessage(), error);
				}
			}
		}
	}

	protected EventCountByCategory(GeoEventProcessorDefinition definition) throws ComponentException
	{
		super(definition);
	}

	public void afterPropertiesSet()
	{
		notificationMode = Validator.valueOfIgnoreCase(EventCountByCategoryNotificationMode.class, getProperty("notificationMode").getValueAsString(), EventCountByCategoryNotificationMode.OnChange);
		reportInterval = Converter.convertToInteger(getProperty("reportInterval").getValueAsString(), 10) * 1000;
		categoryField = getProperty("categoryField").getValueAsString();
		autoResetCounter = Converter.convertToBoolean(getProperty("autoResetCounter").getValueAsString());
		String[] resetTimeStr = getProperty("resetTime").getValueAsString().split(":");

		// Get the Date corresponding to 11:01:00 pm today.
		Calendar calendar = Calendar.getInstance();

		calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(resetTimeStr[0]));
		calendar.set(Calendar.MINUTE, Integer.parseInt(resetTimeStr[1]));
		calendar.set(Calendar.SECOND, Integer.parseInt(resetTimeStr[2]));
		resetTime = calendar.getTime();
		clearCache = Converter.convertToBoolean(getProperty("clearCache").getValueAsString());
	}

	@Override
	public void setId(String id)
	{
		super.setId(id);
		geoEventProducer = messaging.createGeoEventProducer(new EventDestination(id + ":event"));
	}

	@Override
	public GeoEvent process(GeoEvent geoEvent) throws Exception
	{
		String trackId = geoEvent.getTrackId();
		MapGeometry geometry = geoEvent.getGeometry();
		String previousCategory = trackCache.get(trackId);
		String category = (String) geoEvent.getField(new FieldExpression(categoryField)).getValue();

		// Need to synchronize the Concurrent Map on write to avoid wrong counting
		synchronized (lock1)
		{
			// Add or update the status cache
			trackCache.put(trackId, category);
			if (!counterCache.containsKey(category))
			{
				counterCache.put(category, new Counters());
			}

			Counters counters = counterCache.get(category);
			counters.setId(category);
			counters.setGeometry(geometry);
			counters.setBothCurrentAndCumulativeCounts(counters.currentCount + 1, counters.cumulativeCount + 1);
			counterCache.put(category, counters);

			// Adjust the previous current count
			if (previousCategory != null && !category.equals(previousCategory))
			{
				Counters previousCounters = counterCache.get(previousCategory);
				previousCounters.setBothCurrentAndCumulativeCounts(previousCounters.currentCount - 1, counters.cumulativeCount + 1);
				counterCache.put(previousCategory, previousCounters);
			}
		}

		return null;
	}

	@Override
	public List<EventDestination> getEventDestinations()
	{
		return (geoEventProducer != null) ? Arrays.asList(geoEventProducer.getEventDestination()) : new ArrayList<EventDestination>();
	}

	@Override
	public void validate() throws ValidationException
	{
		super.validate();
		List<String> errors = new ArrayList<String>();
		if (reportInterval <= 0)
			errors.add(LOGGER.translate("VALIDATION_INVALID_REPORT_INTERVAL", definition.getName()));
		if (errors.size() > 0)
		{
			StringBuffer sb = new StringBuffer();
			for (String message : errors)
				sb.append(message).append("\n");
			throw new ValidationException(LOGGER.translate("VALIDATION_ERROR", this.getClass().getName(), sb.toString()));
		}
	}

	@Override
	public void onServiceStart()
	{
		if (this.autoResetCounter == true || this.clearCache == true)
		{
			if (clearCacheTimer == null)
			{
				// Get the Date corresponding to 11:01:00 pm today.
				Calendar calendar1 = Calendar.getInstance();
				calendar1.setTime(resetTime);
				Date time1 = calendar1.getTime();

				clearCacheTimer = new Timer();
				Long dayInMilliSeconds = 60 * 60 * 24 * 1000L;
				clearCacheTimer.scheduleAtFixedRate(new ClearCacheTask(), time1, dayInMilliSeconds);
			}
			trackCache.clear();
			counterCache.clear();
		}

		isCounting = true;
		if (definition != null)
		{
			definitionUri = definition.getUri();
			definitionUriString = definitionUri.toString();
		}

		ReportGenerator reportGen = new ReportGenerator(categoryField, reportInterval);
		Thread thread = new Thread(reportGen);
		thread.setName("EventCountByCategory Report Generator");
		thread.start();
	}

	@Override
	public void onServiceStop()
	{
		if (clearCacheTimer != null)
		{
			clearCacheTimer.cancel();
		}
		isCounting = false;
	}

	@Override
	public void shutdown()
	{
		super.shutdown();

		if (clearCacheTimer != null)
		{
			clearCacheTimer.cancel();
		}
	}

	@Override
	public EventDestination getEventDestination()
	{
		return (geoEventProducer != null) ? geoEventProducer.getEventDestination() : null;
	}

	@Override
	public void send(GeoEvent geoEvent) throws MessagingException
	{
		if (geoEventProducer != null && geoEvent != null)
		{
			geoEventProducer.send(geoEvent);
		}
	}

	public void setMessaging(Messaging messaging)
	{
		this.messaging = messaging;
		geoEventCreator = messaging.createGeoEventCreator();
	}

	private GeoEvent createCounterGeoEvent(String matId, Counters counters) throws MessagingException
	{
		GeoEvent counterEvent = null;
		if (geoEventCreator != null && definitionUriString != null && definitionUri != null)
		{
			try
			{
				counterEvent = geoEventCreator.create("EventCountByCategory", definitionUriString);
				counterEvent.setField(0, matId);
				counterEvent.setField(1, counters.cumulativeCount);
				counterEvent.setField(2, counters.currentCount);
				counterEvent.setField(3, new Date());
				counterEvent.setField(4, counters.getGeometry());
				counterEvent.setProperty(GeoEventPropertyName.TYPE, "event");
				counterEvent.setProperty(GeoEventPropertyName.OWNER_ID, getId());
				counterEvent.setProperty(GeoEventPropertyName.OWNER_URI, definitionUri);
			}
			catch (FieldException error)
			{
				counterEvent = null;
				LOGGER.error("CREATE_GEOEVENT_FAILED", error.getMessage());
				LOGGER.info(error.getMessage(), error);
			}
		}
		return counterEvent;
	}

	@Override
	public void disconnect()
	{
		if (geoEventProducer != null)
			geoEventProducer.disconnect();
	}

	@Override
	public String getStatusDetails()
	{
		return (geoEventProducer != null) ? geoEventProducer.getStatusDetails() : "";
	}

	@Override
	public void init() throws MessagingException
	{
		afterPropertiesSet();
	}

	@Override
	public boolean isConnected()
	{
		return (geoEventProducer != null) ? geoEventProducer.isConnected() : false;
	}

	@Override
	public void setup() throws MessagingException
	{
		;
	}

	@Override
	public void update(Observable o, Object arg)
	{
		;
	}
}
