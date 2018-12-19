package io.vertx.cassandra;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * This test ensures that client can be automatically closed on verticle
 * undeploy event.
 */
@RunWith(VertxUnitRunner.class)
public class CloseHookTest extends CassandraServiceBase {

    public static final String TEST_CLIENT_NAME = "TEST_CLIENT";

    @Test
    public void testClientClosedAfterUndeploy(TestContext context) {
        Async async = context.async();
        vertx.deployVerticle(new VerticleWithCassandraClient(), deploy -> {
            if (deploy.succeeded()) {
                vertx.undeploy(deploy.result(), undeploy -> {
                    if (undeploy.succeeded()) {
                        CassandraClient shared = CassandraClient.createShared(vertx, TEST_CLIENT_NAME);
                        context.assertFalse(shared.isConnected());
                        async.complete();
                    } else {
                        context.fail(undeploy.cause());
                    }
                });
            } else {
                context.fail(deploy.cause());
            }
        });
    }

    static class VerticleWithCassandraClient extends AbstractVerticle {

        @Override
        public void start(Future<Void> startFuture) {
            CassandraClientOptions options = new CassandraClientOptions()
                    .addContactPoint(HOST)
                    .setPort(NATIVE_TRANSPORT_PORT);

            CassandraClient shared = CassandraClient.createShared(vertx, TEST_CLIENT_NAME, options);
            startFuture.complete();
        }
    }
}
