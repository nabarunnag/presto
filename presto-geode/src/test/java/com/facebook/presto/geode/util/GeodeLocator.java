package com.facebook.presto.geode.util;

import java.io.IOException;
import java.net.URISyntaxException;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;

public class GeodeLocator implements Runnable {
  private final LocatorLauncher locatorLauncher;

  public static GeodeLocator createLocator()
      throws IOException, URISyntaxException
  {
    return new GeodeLocator();
  }

  GeodeLocator()
      throws IOException, URISyntaxException
  {
    this.locatorLauncher = new LocatorLauncher.Builder()
        .setMemberName("locator")
        .setPort(10334)
        .build();
  }

  public void close()
  {
    locatorLauncher.stop();
  }


  @Override
  public void run() {
    locatorLauncher.start();
  }
}
