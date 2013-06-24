/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.scarcemedia.mapreduce.flume.twitter;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author jeremy
 */
public class TwitterSearchSourceTest {
  private TwitterSearchSource _TwitterSearchSource;
  private ChannelProcessor _ChannelProcessor;
  
  @Before
  public void setup(){
    _TwitterSearchSource = new TwitterSearchSource();
    Context context = new Context();
  }
  
  @Test
  public void asdf() throws EventDeliveryException{
//    _TwitterSearchSource.process();
  }
}
