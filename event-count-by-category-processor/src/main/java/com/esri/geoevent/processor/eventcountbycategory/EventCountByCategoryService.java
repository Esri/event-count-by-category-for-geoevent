package com.esri.geoevent.processor.eventcountbycategory;

import com.esri.ges.core.component.ComponentException;
import com.esri.ges.messaging.Messaging;
import com.esri.ges.processor.GeoEventProcessor;
import com.esri.ges.processor.GeoEventProcessorServiceBase;

public class EventCountByCategoryService extends GeoEventProcessorServiceBase
{
  private Messaging messaging;

  public EventCountByCategoryService()
  {
    definition = new EventCountByCategoryDefinition();
  }

  @Override
  public GeoEventProcessor create() throws ComponentException
  {
    EventCountByCategory detector = new EventCountByCategory(definition);
    detector.setMessaging(messaging);
    return detector;
  }

  public void setMessaging(Messaging messaging)
  {
    this.messaging = messaging;
  }
}