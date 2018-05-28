package edu.ecnu.touchstone.controller;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import org.apache.log4j.Logger;

import edu.ecnu.touchstone.pretreatment.TableGeneTemplate;
import edu.ecnu.touchstone.run.Touchstone;

// the client of the controller
// main functions: send the table generation template (including the referenced 
// primary key join information) to the data generator
public class ControllerClient implements Runnable {

	// the host and port of the data generator server
	private String host = null;
	private int port;

	private Logger logger = null;

	// the message channel that is created after the link is established
	private Channel channel = null;

	public ControllerClient(String host, int port) {
		this.host = host;
		this.port = port;
		logger = Logger.getLogger(Touchstone.class);
	}

	@Override
	public void run() {
		connect();
	}

	// build link
	private void connect() {
		EventLoopGroup group = new NioEventLoopGroup(1);
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(group).channel(NioSocketChannel.class)
		.option(ChannelOption.TCP_NODELAY, true)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				// support the native serialization of Java
				ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.
						weakCachingConcurrentResolver(this.getClass().getClassLoader())));
				ch.pipeline().addLast(new ObjectEncoder());
				ch.pipeline().addLast(new ControllerClientHandler());
			}
		});

		while (true) {
			try {
				Thread.sleep(1000);
				channel = bootstrap.connect(host, port).sync().channel();
				break;
			} catch (Exception e) {
				logger.error("\n\tController client startup fail!");
			}
		}
		logger.debug("\n\tController client startup successful!");
	}

	public void send(TableGeneTemplate template) {
		channel.writeAndFlush(template);
	}

	public boolean isConnected() {
		return channel != null ? true : false;
	}
}
