package com.facebook.presto.geode.util;

import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.cache.GemFireCacheImpl;

public class PopulateServer {
  public static void main(String[] args) {
    String tableName = "region";
    System.out.println("Doing puts");
    JsonEncoder jsonEncoder = new JsonEncoder();
    ClientCacheFactory
        clientCacheFactory = new ClientCacheFactory().set("cache-xml-file","/Users/nnag/Development/prestodb/presto/presto-geode/src/test/java/com/facebook/presto/geode/util/client-cache.xml");
    ClientCache clientCache = clientCacheFactory.create();
    Region region = clientCache.getRegion("default.region");
    for (long i = 0; i < 1000; i++) {
      Object value = ImmutableMap.of("id", Long.toString(i), "value", UUID.randomUUID().toString());

      {
        region.put(tableName + ":" + i, jsonEncoder.toString(value));
      }
    }
    System.out.println("Done with puts");
  }
}
