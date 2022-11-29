package clickhouse.mysqlengine.odbc.proxy;


import io.netty.util.internal.StringUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;


/**
 * @author shaoyunchuan
 * @date 2022/11/24
 */
public class ClickhouseMysqlEngineODBCProxyServer {
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseMysqlEngineODBCProxyServer.class);

    public static void main(String[] args) {
        Vertx.vertx().deployVerticle(new ClickhouseMysqlEngineODBCProxyServerVerticle());
    }

    public static class ClickhouseMysqlEngineODBCProxyServerVerticle extends AbstractVerticle {

        private final String clickhouseHost;
        private final int clickhousePort;


        ClickhouseMysqlEngineODBCProxyServerVerticle() {
            String clickhouseHost = System.getProperty("clickhouse.host");
            if (StringUtil.isNullOrEmpty(clickhouseHost)) {
                clickhouseHost = "127.0.0.1";
            }
            this.clickhouseHost = clickhouseHost;

            String clickhousePort = System.getProperty("clickhouse.port");
            if (StringUtil.isNullOrEmpty(clickhousePort)) {
                clickhousePort = "9004";
            }
            this.clickhousePort = Integer.parseInt(clickhousePort);

        }

        @Override
        public void start() {
            NetServer netServer = vertx.createNetServer();//创建代理服务器
            NetClient netClient = vertx.createNetClient();//创建连接clickhouse客户端
            netServer.connectHandler(socket -> netClient.connect(clickhousePort, clickhouseHost, result -> {
                //响应来自客户端的连接请求，成功之后，在建立一个与目标clickhouse服务器的连接
                if (result.succeeded()) {
                    //与目标clickhouse服务器成功连接连接之后，创造一个ClickhouseMysqlEngineODBCProxyConnection对象,并执行代理方法
                    new ClickhouseMysqlEngineODBCProxyConnection(socket, result.result()).proxy();
                } else {
                    logger.error(result.cause().getMessage(), result.cause());
                    socket.close();
                }
            })).listen(clickhousePort, listenResult -> {//代理服务器的监听端口
                if (listenResult.succeeded()) {
                    //成功启动代理服务器
                    logger.info("Clickhouse proxy server start up. host:" + this.clickhouseHost + " port:" + this.clickhousePort);
                } else {
                    //启动代理服务器失败
                    logger.error("Clickhouse proxy exit. because: " + listenResult.cause().getMessage(), listenResult.cause() + " host:" + this.clickhouseHost + " port:" + this.clickhousePort);
                    System.exit(1);
                }
            });
        }
    }


    public static class ClickhouseMysqlEngineODBCProxyConnection {
        private final NetSocket clientSocket;
        private final NetSocket serverSocket;
        private final ClickHouseMysqlEngineODBCHandler engineHandler;

        public ClickhouseMysqlEngineODBCProxyConnection(NetSocket clientSocket, NetSocket serverSocket) {
            this.clientSocket = clientSocket;
            this.serverSocket = serverSocket;
            this.engineHandler = new ClickHouseMysqlEngineODBCHandler();
        }

        private void proxy() {
            //当代理与clickhouse服务器连接关闭时，关闭client与代理的连接
            serverSocket.closeHandler(v -> clientSocket.close());
            //反之亦然
            clientSocket.closeHandler(v -> serverSocket.close());
            //不管那端的连接出现异常时，关闭两端的连接
            serverSocket.exceptionHandler(e -> {
                logger.error(e.getMessage(), e);
                close();
            });
            clientSocket.exceptionHandler(e -> {
                logger.error(e.getMessage(), e);
                close();
            });
            //当收到来自客户端的数据包时，转发给clickhouse目标服务器
            clientSocket.handler(buffer ->
                    serverSocket.write(buffer)
            );

            //当收到来自clickhouse目标服务器的数据包时，转发给客户端
            serverSocket.handler(buffer -> {
                        try {
                            byte[] bytes = buffer.getBytes();
                            // 排除挥手报文与登录报文
                            if ((this.engineHandler.isEmpty() && bytes[0] == 1)
                                    || !this.engineHandler.isEmpty()) {

                                this.engineHandler.append(bytes);
                                if (this.engineHandler.hasCompleted()) {
                                    // 确保数据集完整后，开始补充字段最大长度与转换字段类型
                                    engineHandler.handler();
                                    // 推流
                                    clientSocket.write(BufferImpl.buffer(engineHandler.read()));
                                    // 清空handler
                                    this.engineHandler.clear();
                                }
                                return;
                            }

                            clientSocket.write(buffer);
                        } catch (Exception e) {
                            close();
                            logger.error(e.getMessage());
                        }

                    }

            );
        }


        private void close() {
            engineHandler.clear();
            try {
                clientSocket.close();
            } catch (Exception ignored) {
            }
            try {
                serverSocket.close();
            } catch (Exception ignored) {
            }
        }


    }
}



