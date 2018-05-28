package edu.ecnu.touchstone.controller;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import org.apache.log4j.Logger;

import edu.ecnu.touchstone.run.Touchstone;

// the server of the controller
// main functions: receive the join information of primary keys sent from the data generator
public class ControllerServer implements Runnable {
	
	// the port of the controller server 
	private int port;
	private Logger logger = null;

	public ControllerServer(int port) {
		this.port = port;
		logger = Logger.getLogger(Touchstone.class);
	}
	
	@Override
	public void run() {
		bind();
	}

	// set up the server of the controller
	private void bind() {
		// configuring the NIO thread groups of the server
		// NioEventLoopGroup is a thread group, and it's specialized for 
		//     network event processing (it is actually a 'Reactor' thread group)
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(1);
		try {
			// ServerBootstrap is an assistant class of Netty for setting up the NIO server
			ServerBootstrap bootstrap = new ServerBootstrap();
			bootstrap.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			// maximum number of connections
			.option(ChannelOption.SO_BACKLOG, 1024)
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) {
					ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.
							weakCachingConcurrentResolver(this.getClass().getClassLoader())));
					ch.pipeline().addLast(new ObjectEncoder());
					ch.pipeline().addLast(new ControllerServerHandler());
				}
			});

			// bind the port and wait for synchronization to succeed
			ChannelFuture cf = bootstrap.bind(port).sync();
			logger.info("\n\tController server startup successful!");

			// wait for server-side listening port to close
			// if there is no this line, server would be closed right away after setting up 
			cf.channel().closeFuture().sync();
		} catch(Exception e) { 
			e.printStackTrace();
		} finally {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
}
