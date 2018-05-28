package edu.ecnu.touchstone.datagenerator;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.ecnu.touchstone.run.Touchstone;
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

// the client of the data generator
// main functions: send the 'pkJoinInfo' to the controller
public class DataGeneratorClient implements Runnable {
	
	// the host and port of the controller server
	private String host = null;
	private int port;
	
	private Logger logger = null;
	
	// the message channel that is created after the link is established
	private static Channel channel = null;
	
	public DataGeneratorClient(String host, int port) {
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
		// configuring the NIO thread groups of the server
		EventLoopGroup group = new NioEventLoopGroup(1);
		// Bootstrap is an assistant class of Netty for setting up client
		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(group).channel(NioSocketChannel.class)
		.option(ChannelOption.TCP_NODELAY, true)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.
						weakCachingConcurrentResolver(this.getClass().getClassLoader())));
				ch.pipeline().addLast(new ObjectEncoder());
				ch.pipeline().addLast(new DataGeneratorClientHandler());
			}
		});
		
		while (true) {
			try {
				Thread.sleep(1000);
				channel = bootstrap.connect(host, port).sync().channel();
				break;
			} catch (Exception e) {
				logger.info("\n\tData generator client startup fail!");
			}
		}
		logger.info("\n\tData generator client startup successful!");
	}
	
	public void send(Map<Integer, ArrayList<long[]>> pkJoinInfo) {
		channel.writeAndFlush(pkJoinInfo);
	}
}
