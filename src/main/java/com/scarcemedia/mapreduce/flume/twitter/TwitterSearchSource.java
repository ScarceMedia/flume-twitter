/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.scarcemedia.mapreduce.flume.twitter;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author jeremy
 */
public class TwitterSearchSource extends AbstractSource implements Configurable, EventDrivenSource {

  private final static Log log = LogFactory.getLog(TwitterSearchSource.class);
  private TwitterStreamFactory _TwitterFactory;
  private TwitterStream _TwitterStream;
  private FlumeEventStatusListener _FlumeListener;

  public void configure(Context context) {
    ConfigurationBuilder builder = new ConfigurationBuilder();
    builder.setJSONStoreEnabled(true);

    boolean debugenabled = context.getBoolean("twitterdebugenabled", false);
    builder.setDebugEnabled(debugenabled);

    String consumerKey = context.getString("consumerkey");
    Preconditions.checkArgument(null != consumerKey, "consumerkey must be configured.");
    if (null != consumerKey && !consumerKey.isEmpty()) {
      builder.setOAuthConsumerKey(consumerKey);
    }

    String consumerSecret = context.getString("consumersecret");
    Preconditions.checkArgument(null != consumerSecret, "consumersecret must be configured.");
    if (null != consumerKey && !consumerKey.isEmpty()) {
      builder.setOAuthConsumerSecret(consumerSecret);
    }

    String accesstoken = context.getString("accesstoken");
    Preconditions.checkArgument(null != accesstoken && !accesstoken.isEmpty(), "accesstoken cannot be null or empty.");
    builder.setOAuthAccessToken(accesstoken);

    String accessTokenSecret = context.getString("accesstokensecret");
    Preconditions.checkArgument(null != accessTokenSecret && !accessTokenSecret.isEmpty(), "accesstokensecret cannot be null or empty.");
    builder.setOAuthAccessTokenSecret(accessTokenSecret);

    String proxyHost = context.getString("proxyhost");

    if (null != proxyHost && !proxyHost.isEmpty()) {
      builder.setHttpProxyHost(proxyHost);

      int proxyPort = context.getInteger("proxyport", -1);
      Preconditions.checkArgument(proxyPort > -1, "proxyport must be specified if proxyhost is specified");
      builder.setHttpProxyPort(proxyPort);

      String proxyUser = context.getString("proxyuser");
      if (null != proxyUser && !proxyUser.isEmpty()) {
        builder.setHttpProxyUser(proxyUser);

        String proxyPassword = context.getString("proxypass");
        builder.setHttpProxyPassword(proxyPassword);
      }
    }

    _TwitterFactory = new TwitterStreamFactory(builder.build());
    _TwitterStream = _TwitterFactory.getInstance();
    _FlumeListener = new FlumeEventStatusListener(this);
    _TwitterStream.addListener(_FlumeListener);

    FilterQuery filter = new FilterQuery();
    
    
    Map<String, String> mapQueries = context.getSubProperties("track");

    if(!mapQueries.isEmpty()){
      String[] track = new String[mapQueries.size()];
      int i=0;
      for(Map.Entry<String, String> keyvalue:mapQueries.entrySet()){
        track[i++] = keyvalue.getValue();
      }
      
      filter.track(track);
    }

  }

  @Override
  public synchronized void start() {

    super.start();
    if (log.isInfoEnabled()) {
      log.info("Calling TwitterStream.filter()");
    }
    FilterQuery filter = new FilterQuery();
    filter.track(new String[]{"costco", "#costco"});

    
    _TwitterStream.filter(filter);
  }

  @Override
  public synchronized void stop() {
    _TwitterStream.shutdown();
    super.stop();
  }
}
