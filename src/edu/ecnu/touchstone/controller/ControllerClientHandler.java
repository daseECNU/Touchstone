package edu.ecnu.touchstone.controller;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.apache.log4j.Logger;

import edu.ecnu.touchstone.run.Touchstone;

public class ControllerClientHandler extends ChannelInboundHandlerAdapter {
	
	private Logger logger = null;

	public ControllerClientHandler() {
		logger = Logger.getLogger(Touchstone.class);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		String response = (String)msg;
		logger.info("\n\t" + response);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}
}
