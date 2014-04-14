package com.esri.geoevent.processor.eventcountbycategory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.core.geoevent.FieldException;
import com.esri.ges.core.geoevent.FieldExpression;
import com.esri.ges.core.geoevent.GeoEvent;
import com.esri.ges.core.geoevent.GeoEventPropertyName;
import com.esri.ges.core.validation.ValidationException;
import com.esri.ges.messaging.EventDestination;
import com.esri.ges.messaging.EventProducer;
import com.esri.ges.messaging.EventUpdatable;
import com.esri.ges.messaging.GeoEventCreator;
import com.esri.ges.messaging.GeoEventProducer;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.messaging.MessagingException;
import com.esri.ges.processor.GeoEventProcessorBase;
import com.esri.ges.processor.GeoEventProcessorDefinition;
import com.esri.ges.util.Converter;
import com.esri.ges.util.Validator;

public class EventCountByCategory extends GeoEventProcessorBase implements Observer, EventProducer, EventUpdatable
{
  private static final Log                     log                      = LogFactory.getLog(EventCountByCategory.class);
  private EventCountByCategoryNotificationMode notificationMode;
  private long                                 reportInterval;

  private final Map<String, EventCountMonitor> eventCountMonitors       = new ConcurrentHashMap<String, EventCountMonitor>();

  private final Map<String, Thread>            eventCountMonitorThreads = new ConcurrentHashMap<String, Thread>();

  private final Map<String, String>            trackCache               = new ConcurrentHashMap<String, String>();
  private final Map<String, Long>              categoryCountCache       = new ConcurrentHashMap<String, Long>();

  private Messaging                            messaging;
  private GeoEventCreator                      geoEventCreator;
  private GeoEventProducer                     geoEventProducer;
  private EventDestination                     destination;
  private Date                                 resetTime;
  private boolean                              autoResetCounter;
  private boolean                              clearCache;
  private Timer                                clearCacheTimer;
  private String                               categoryField;

  class ClearCacheTask extends TimerTask
  {
    public void run()
    {
      if (autoResetCounter == true)
      {
        for (EventCountMonitor monitor : eventCountMonitors.values())
        {
          monitor.setEventCount(0);
        }
      }
      // clear the cache
      if (clearCache == true)
      {
        for (EventCountMonitor monitor : eventCountMonitors.values())
        {
          monitor.stop();
          monitor.stopMonitoring();
        }
        eventCountMonitors.clear();
        eventCountMonitorThreads.clear();
      }
    }
  }

  protected EventCountByCategory(GeoEventProcessorDefinition definition) throws ComponentException
  {
    super(definition);
  }

  public void afterPropertiesSet()
  {
    notificationMode = Validator.validateEnum(EventCountByCategoryNotificationMode.class, getProperty("notificationMode").getValueAsString(), EventCountByCategoryNotificationMode.OnChange);
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
    destination = new EventDestination(getId() + ":event");
    geoEventProducer = messaging.createGeoEventProducer(destination.getName());
  }

  @Override
  public GeoEvent process(GeoEvent geoEvent) throws Exception
  {
    String trackId = geoEvent.getTrackId();
    String previousCategory = trackCache.get(trackId);
    String category = (String) geoEvent.getField(new FieldExpression(categoryField)).getValue();

    // Add or update the status cache
    trackCache.put(trackId, category);
    if (!categoryCountCache.containsKey(category))
      categoryCountCache.put(category, 0l);
    
    categoryCountCache.put(category, categoryCountCache.get(category) + 1);
    if (previousCategory != null && !category.equals(previousCategory))
    {
      categoryCountCache.put(previousCategory, categoryCountCache.get(previousCategory) - 1);
    }
    
    doCountMonitoringAndReporting(geoEvent, previousCategory);
    return null;
  }

  @Override
  public List<EventDestination> getEventDestinations()
  {
    return Arrays.asList(destination);
  }

  @Override
  public void validate() throws ValidationException
  {
    super.validate();
    List<String> errors = new ArrayList<String>();
    if (reportInterval <= 0)
      errors.add("'" + definition.getName() + "' property 'reportInterval' is invalid.");
    if (errors.size() > 0)
    {
      StringBuffer sb = new StringBuffer();
      for (String message : errors)
        sb.append(message).append("\n");
      throw new ValidationException(this.getClass().getName() + " validation failed: " + sb.toString());
    }
  }

  @Override
  public void update(Observable observable, Object event)
  {
    if (event instanceof EventCountByCategoryEvent)
    {
      EventCountByCategoryEvent counterEvent = (EventCountByCategoryEvent) event;
      if (counterEvent.isStopMonitoring())
        stopMonitoring(counterEvent.getCategory());
      else
      {
        try
        {
          send(createEventCounterGeoEvent(counterEvent));
        }
        catch (MessagingException e)
        {
          log.error("Failed to send Event Count GeoEvent: ", e);
        }
      }
    }
    notifyObservers(event);
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
        clearCacheTimer.scheduleAtFixedRate(new ClearCacheTask(), time1, 8640000L);
      }
      trackCache.clear();
      categoryCountCache.clear();
    }

    for (EventCountMonitor monitor : eventCountMonitors.values())
      monitor.start();
  }

  @Override
  public void onServiceStop()
  {
    for (EventCountMonitor monitor : eventCountMonitors.values())
      monitor.stop();

    if (clearCacheTimer != null)
    {
      clearCacheTimer.cancel();
    }
  }

  @Override
  public void shutdown()
  {
    super.shutdown();
    for (EventCountMonitor monitor : eventCountMonitors.values())
    {
      monitor.stop();
      monitor.stopMonitoring();
    }
    eventCountMonitors.clear();
    eventCountMonitorThreads.clear();

    if (clearCacheTimer != null)
    {
      clearCacheTimer.cancel();
    }
  }

  @Override
  public EventDestination getEventDestination()
  {
    return destination;
  }

  @Override
  public void send(GeoEvent geoEvent) throws MessagingException
  {
    if (geoEventProducer != null && geoEvent != null)
      geoEventProducer.send(geoEvent);
  }

  private void doCountMonitoringAndReporting(GeoEvent geoEvent, String previousStatus)
  {
    if (trackCache.containsKey(geoEvent.getTrackId()))
    {
      String newStatus = (String) geoEvent.getField(new FieldExpression(categoryField)).getValue();
      EventCountMonitor monitor = null;
      if (eventCountMonitors.containsKey(newStatus))
      {
        monitor = eventCountMonitors.get(newStatus);
      }
      else
      {
        monitor = new EventCountMonitor(geoEvent, notificationMode, reportInterval, autoResetCounter, resetTime, categoryField);
        monitor.addObserver(this);
        eventCountMonitors.put(newStatus, monitor);
        eventCountMonitorThreads.put(newStatus, new Thread(monitor, newStatus));
      }
      if (monitor != null && !monitor.isMonitoring())
      {
        eventCountMonitorThreads.get(newStatus).start();
      }

      if (!categoryCountCache.isEmpty())
      {
        monitor.setEventCount(categoryCountCache.get(newStatus));
      }

      EventCountMonitor prevMonitor = null;
      if (previousStatus != null)
      {
        if (eventCountMonitors.containsKey(previousStatus))
        {
          prevMonitor = eventCountMonitors.get(previousStatus);
          if (!categoryCountCache.isEmpty())
          {
            prevMonitor.setEventCount(categoryCountCache.get(previousStatus));
          }
        }
      }
    }
  }

  private void stopMonitoring(String category)
  {
    if (category != null && eventCountMonitors.containsKey(category))
    {
      eventCountMonitors.remove(category).stopMonitoring();
      eventCountMonitorThreads.remove(category).interrupt();
    }
  }

  private GeoEvent createEventCounterGeoEvent(EventCountByCategoryEvent event) throws MessagingException
  {
    GeoEvent counterEvent = null;
    if (geoEventCreator != null)
    {
      try
      {
        String category = event.getCategory();
        counterEvent = geoEventCreator.create("EventCountByCategory", definition.getUri().toString());
        counterEvent.setField(0, category);
        counterEvent.setField(1, event.getEventCount());
        counterEvent.setField(2, new Date());
        counterEvent.setField(3, event.getGeometry());
        counterEvent.setProperty(GeoEventPropertyName.TYPE, "event");
        counterEvent.setProperty(GeoEventPropertyName.OWNER_ID, getId());
        counterEvent.setProperty(GeoEventPropertyName.OWNER_URI, definition.getUri());
      }
      catch (FieldException e)
      {
        counterEvent = null;
        log.error("Failed to create Event Count by Category GeoEvent: " + e.getMessage());
      }
    }
    return counterEvent;
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
    geoEventCreator = messaging.createGeoEventCreator();
  }
}

final class EventCountMonitor extends Observable implements Runnable
{
  private boolean                              monitoring;
  private boolean                              running;
  private GeoEvent                             geoEvent;
  private EventCountByCategoryNotificationMode notificationMode;
  private long                                 reportInterval;
  private long                                 eventCount;
  private boolean                              autoResetCounter;
  private Date                                 resetTime;
  private String                               categoryField;
  private boolean                              changeDetected = false;

  protected EventCountMonitor(GeoEvent geoEvent, EventCountByCategoryNotificationMode notificationMode, long reportInterval, boolean autoResetCounter, Date resetTime, String categoryField)
  {
    this.geoEvent = geoEvent;
    this.eventCount = 0;
    this.monitoring = false;
    this.categoryField = categoryField;
    this.running = true;
    setNotificationMode(notificationMode);
    setTimeInterval(reportInterval);
    setAutoResetCounter(autoResetCounter);
    setResetTime(resetTime);
  }

  public EventCountByCategoryNotificationMode getNotificationMode()
  {
    return notificationMode;
  }

  public void setNotificationMode(EventCountByCategoryNotificationMode notificationMode)
  {
    this.notificationMode = (notificationMode != null) ? notificationMode : EventCountByCategoryNotificationMode.OnChange;
  }

  public long getTimeInterval()
  {
    return reportInterval;
  }

  public void setTimeInterval(long timeInterval)
  {
    this.reportInterval = (timeInterval > 0) ? timeInterval : 120000;
  }

  public long getEventCount()
  {
    return eventCount;
  }

  public void setEventCount(long eventCount)
  {
    if (this.eventCount != eventCount)
    {
      this.changeDetected = true;
      this.eventCount = eventCount;
    }
    else
    {
      this.changeDetected = false;
    }
  }

  @Override
  public void run()
  {
    monitoring = true;
    while (monitoring)
    {
      String category = (String) geoEvent.getField(new FieldExpression(categoryField)).getValue();
      try
      {
        if (running)
        {
          switch (notificationMode)
          {
            case OnChange:
              if (this.changeDetected == true)
              {
                //Thread.sleep(1); //Sleep 1 millisecond to prevent tight loop
                notifyObservers(new EventCountByCategoryEvent(this.geoEvent.getGeometry(), category, this.eventCount, false));
                consoleDebugPrintLn(category + ":" + this.eventCount);
              }
              break;
            case Continuous:
              Thread.sleep(reportInterval);
              notifyObservers(new EventCountByCategoryEvent(this.geoEvent.getGeometry(), category, this.eventCount, false));
              consoleDebugPrintLn(category + ":" + this.eventCount);
              break;
          }
          // reset the changeDeteced flag
          this.changeDetected = false;
        }
        else
        {
          notifyObservers(new EventCountByCategoryEvent(this.geoEvent.getGeometry(), category, 0, true));
        }
      }
      catch (InterruptedException e)
      {
        stopMonitoring();
      }
    }
  }

  public boolean isMonitoring()
  {
    return monitoring;
  }

  public void stopMonitoring()
  {
    monitoring = false;
  }

  public void start()
  {
    running = true;
  }

  public void stop()
  {
    running = false;
  }

  @Override
  public void notifyObservers(Object event)
  {
    if (event != null)
    {
      setChanged();
      super.notifyObservers(event);
      clearChanged();
    }
  }

  public boolean isAutoResetCounter()
  {
    return autoResetCounter;
  }

  public void setAutoResetCounter(boolean autoResetCounter)
  {
    this.autoResetCounter = autoResetCounter;
  }

  public Date getResetTime()
  {
    return resetTime;
  }

  public void setResetTime(Date resetTime2)
  {
    this.resetTime = resetTime2;
  }

  public static void consoleDebugPrintLn(String msg)
  {
    String consoleOut = System.getenv("GEP_CONSOLE_OUTPUT");
    if (consoleOut != null && "1".equals(consoleOut))
    {
      System.out.println(msg);
    }
  }

  public static void consoleDebugPrint(String msg)
  {
    String consoleOut = System.getenv("GEP_CONSOLE_OUTPUT");
    if (consoleOut != null && "1".equals(consoleOut))
    {
      System.out.print(msg);
    }
  }
}
