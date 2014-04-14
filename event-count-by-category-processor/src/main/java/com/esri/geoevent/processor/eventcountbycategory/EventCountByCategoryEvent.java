package com.esri.geoevent.processor.eventcountbycategory;

import com.esri.ges.spatial.Geometry;

public class EventCountByCategoryEvent
{
  private Geometry geometry;
	private String category;
	private long eventCount;
	private boolean stopMonitoring;

	public EventCountByCategoryEvent(Geometry geometry, String category, long eventCount, boolean stopMonitoring)
	{
	  this.geometry = geometry;
		this.category = category;
		this.eventCount = eventCount;
		this.stopMonitoring = stopMonitoring;
	}

	public Geometry getGeometry()
	{
	  return geometry;
	}
	
	public String getCategory()
	{
		return category;
	}

	public long getEventCount()
	{
		return eventCount;
	}

	public boolean isStopMonitoring()
	{
		return stopMonitoring;
	}

	@Override
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append("EventCountByCategoryEvent(");
		sb.append(category);
		sb.append(", ");
		sb.append(eventCount);
		sb.append(")");
		return sb.toString();
	}
}