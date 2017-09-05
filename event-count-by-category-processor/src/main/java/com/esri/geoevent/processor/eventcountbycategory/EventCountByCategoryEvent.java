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

import com.esri.core.geometry.MapGeometry;

public class EventCountByCategoryEvent
{
  private MapGeometry geometry;
	private String category;
	private Long currentCounter;
  private Long cumulativeCounter;
	private boolean stopMonitoring;

	public EventCountByCategoryEvent(MapGeometry geometry, String category, Long currentCounter, Long cumulativeCounter, boolean stopMonitoring)
	{
	  this.geometry = geometry;
		this.category = category;
		this.setCurrentCounter(currentCounter);
		this.setCumulativeCounter(cumulativeCounter);
		this.stopMonitoring = stopMonitoring;
	}

	public MapGeometry getGeometry()
	{
	  return geometry;
	}
	
	public String getCategory()
	{
		return category;
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
		sb.append(getCurrentCounter());
    sb.append(", ");
    sb.append(getCumulativeCounter());
		sb.append(")");
		return sb.toString();
	}

  public Long getCurrentCounter()
  {
    return currentCounter;
  }

  public void setCurrentCounter(Long currentCounter)
  {
    this.currentCounter = currentCounter;
  }

  public Long getCumulativeCounter()
  {
    return cumulativeCounter;
  }

  public void setCumulativeCounter(Long cumulativeCounter)
  {
    this.cumulativeCounter = cumulativeCounter;
  }
}