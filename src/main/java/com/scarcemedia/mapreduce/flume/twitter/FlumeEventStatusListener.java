/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.scarcemedia.mapreduce.flume.twitter;

import com.google.common.base.Preconditions;
import java.nio.charset.Charset;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Event;
import org.apache.flume.Source;
import org.apache.flume.event.EventBuilder;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author jeremy
 */
public class FlumeEventStatusListener implements StatusListener {
  
  private static Log log = LogFactory.getLog(FlumeEventStatusListener.class);
  private static final Charset _Charset = Charset.forName("UTF-8");
  private Source _Source;
  
  public FlumeEventStatusListener(Source source) {
    Preconditions.checkArgument(null != source, "source cannot be null");
    _Source = source;
  }
  
  private long _StatusesProcessed = 0L;
  
  public void onStatus(Status status) {    
    try {
      Event event = EventBuilder.withBody(DataObjectFactory.getRawJSON(status), _Charset);
      _Source.getChannelProcessor().processEvent(event);
      
      if(_StatusesProcessed % 50000==0&&log.isInfoEnabled()){
        log.info("Processed " + _StatusesProcessed + " status(es).");
      }
      
    } catch (Exception ex) {
      if (log.isInfoEnabled()) {
        log.info("Exception while processing status", ex);
      }
    } finally {
      _StatusesProcessed++;
    }
  }
  
  public void onDeletionNotice(StatusDeletionNotice sdn) {
    
  }
  
  public void onTrackLimitationNotice(int i) {
    if(log.isInfoEnabled()) {
      log.info("onTrackLimitationNotice - " + i);
    }
  }
  
  public void onScrubGeo(long l, long l1) {
    
  }
  
  public void onStallWarning(StallWarning sw) {
    
  }
  
  public void onException(Exception ex) {
    if (log.isErrorEnabled()) {
      log.error("Exception thrown", ex);
    }
  }
}
