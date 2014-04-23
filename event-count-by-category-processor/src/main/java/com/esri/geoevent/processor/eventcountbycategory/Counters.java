package com.esri.geoevent.processor.eventcountbycategory;

class Counters
{
  /**
   * 
   */
  Long cumulativeCounter = 0L;
  Long currentCounter = 0L;
  
  public Counters()
  {
  }

  public Long getCumulativeCounter()
  {
    return cumulativeCounter;
  }

  public void setCumulativeCounter(Long cumulativeCounter)
  {
    this.cumulativeCounter = cumulativeCounter;
  }

  public Long getCurrentCounter()
  {
    return currentCounter;
  }

  public void setCurrentCounter(Long currentCounter)
  {
    this.currentCounter = currentCounter;
  }   
}