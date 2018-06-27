/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.geode.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.distributed.ServerLauncher;

public class GeodeServer
        implements Closeable
{
    private final ServerLauncher serverLauncher;

    public static GeodeServer createGeodeServerLauncher()
            throws IOException, URISyntaxException
    {
        return new GeodeServer();
    }

    GeodeServer()
            throws IOException, URISyntaxException
    {
        this.serverLauncher = new ServerLauncher.Builder()
            .setMemberName("server")
            .setServerPort(40404)
            .set("cache-xml-file","/Users/nnag/Development/prestodb/presto/presto-geode/src/test/java/com/facebook/presto/geode/util/server-cache.xml")
            .set("locators","localhost[10334]")
            .build();

    }

    public void start()
        throws IOException, URISyntaxException {
        GeodeLocator.createLocator().run();
        serverLauncher.start();

    }



    @Override
    public void close()
    {
       serverLauncher.stop();
    }

    public int getPort()
    {
        return serverLauncher.getServerPort();
    }

    public String getConnectString()
    {
        return serverLauncher.getHostNameForClients();
    }
}
