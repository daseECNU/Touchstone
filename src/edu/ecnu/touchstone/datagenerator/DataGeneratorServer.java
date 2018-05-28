package edu.ecnu.touchstone.datagenerator;

import org.apache.log4j.Logger;

import edu.ecnu.touchstone.run.Touchstone;
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

// the server of the data generator
//main functions: receive the data generation task (TableGeneTemplate) sent from the controller
public class DataGeneratorServer implements Runnable {
	
	// the port of the data generator server 
	private int port;
	private Logger logger = null;

	public DataGeneratorServer(int port) {
		this.port = port;
		logger = Logger.getLogger(Touchstone.class);
	}

	@Override
	public void run() {
		bind();
	}
	
	// set up the server of the data generator
	private void bind() {
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup(1);
		try {
			ServerBootstrap bootstrap = new ServerBootstrap();
			bootstrap.group(bossGroup, workerGroup)
			.channel(NioServerSocketChannel.class)
			.option(ChannelOption.SO_BACKLOG, 1024)
			.childHandler(new ChannelInitializer<SocketChannel>() {
				@Override
				public void initChannel(SocketChannel ch) {
					ch.pipeline().addLast(new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.
							weakCachingConcurrentResolver(this.getClass().getClassLoader())));
					ch.pipeline().addLast(new ObjectEncoder());
					ch.pipeline().addLast(new DataGeneratorServerHandler());
				}
			});

			ChannelFuture cf = bootstrap.bind(port).sync();
			logger.info("\n\tData generator server startup successful!");
			cf.channel().closeFuture().sync();
		} catch(Exception e) { 
			e.printStackTrace();
		} finally {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
}
