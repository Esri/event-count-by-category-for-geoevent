package com.esri.geoevent.processor.eventcountbycategory;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.esri.ges.core.geoevent.DefaultFieldDefinition;
import com.esri.ges.core.geoevent.DefaultGeoEventDefinition;
import com.esri.ges.core.geoevent.FieldDefinition;
import com.esri.ges.core.geoevent.FieldType;
import com.esri.ges.core.geoevent.GeoEventDefinition;
import com.esri.ges.core.property.PropertyDefinition;
import com.esri.ges.core.property.PropertyType;
import com.esri.ges.processor.GeoEventProcessorDefinitionBase;

public class EventCountByCategoryDefinition extends GeoEventProcessorDefinitionBase
{
	final private static Log LOG = LogFactory.getLog(EventCountByCategoryDefinition.class);

	public EventCountByCategoryDefinition()
	{
		try
		{
			propertyDefinitions.put("notificationMode", new PropertyDefinition("notificationMode", PropertyType.String,
					"OnChange", "Count Notification Mode", "Count Notification Mode", true, false, "OnChange",
					"Continuous"));
			propertyDefinitions.put("reportInterval", new PropertyDefinition("reportInterval", PropertyType.Long, 10,
					"Report Interval (seconds)", "Report Interval (seconds)", "notificationMode=Continuous", false, false));
			propertyDefinitions.put("geometryField", new PropertyDefinition("geometryField", PropertyType.String, "GEOMETRY",
					"Geometry Field Name", "Geometry Field Name", false, false));
      propertyDefinitions.put("categoryField", new PropertyDefinition("categoryField", PropertyType.String, "TRACK_ID",
          "Category Field Name", "Category Field Name", false, false));
			propertyDefinitions.put("autoResetCounter", new PropertyDefinition("autoResetCounter", PropertyType.Boolean, false, 
					"Automatic Reset Counter", "Auto Reset Counter", true, false));
			propertyDefinitions.put("resetTime", new PropertyDefinition("resetTime", PropertyType.String, "00:00:00",
					"Reset Counter to Zero at", "Reset Counter time", "autoResetCounter=true", false, false));
			propertyDefinitions.put("clearCache", new PropertyDefinition("clearCache", PropertyType.Boolean, true, 
					"Clear in-memory Cache", "Clear in-memory Cache", "autoResetCounter=true", false, false));
			// TODO: How about TrackId selection to potentially track only a
			// subset of geoevents ???
			GeoEventDefinition ged = new DefaultGeoEventDefinition();
			ged.setName("EventCountByCategory");
			List<FieldDefinition> fds = new ArrayList<FieldDefinition>();
			fds.add(new DefaultFieldDefinition("trackId", FieldType.String, "TRACK_ID"));
			fds.add(new DefaultFieldDefinition("currentCount", FieldType.Long));
      fds.add(new DefaultFieldDefinition("cumulativeCount", FieldType.Long));
			fds.add(new DefaultFieldDefinition("lastReceived", FieldType.Date));
			fds.add(new DefaultFieldDefinition("geometry", FieldType.Geometry, "GEOMETRY"));
			ged.setFieldDefinitions(fds);
			geoEventDefinitions.put(ged.getName(), ged);
		} catch (Exception e)
		{
			LOG.error("Error setting up Event Counter Definition.", e);
		}
	}

	@Override
	public String getName()
	{
		return "EventCountByCategory";
	}

	@Override
	public String getLabel()
	{
		return "Event Count by Category";
	}

	@Override
	public String getDescription()
	{
		return "Counting number of GeoEvents from a Track by category in a specified period of time.";
	}
}