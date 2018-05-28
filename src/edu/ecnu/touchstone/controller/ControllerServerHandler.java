package edu.ecnu.touchstone.controller;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.ecnu.touchstone.run.Touchstone;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ControllerServerHandler extends ChannelInboundHandlerAdapter {
	
	private Logger logger = null;

	public ControllerServerHandler() {
		logger = Logger.getLogger(Touchstone.class);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		@SuppressWarnings("unchecked")
		Map<Integer, ArrayList<long[]>> pkJoinInfo = (Map<Integer, ArrayList<long[]>>)msg;
		Controller.receivePkJoinInfo(pkJoinInfo);
		
		logger.info("\n\tController receives a 'pkJoinInfo' from a data generator!");
		String response = "It's a response of the controller: I has received a 'pkJoinInfo' from you!";
		ctx.writeAndFlush(response);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}
}
