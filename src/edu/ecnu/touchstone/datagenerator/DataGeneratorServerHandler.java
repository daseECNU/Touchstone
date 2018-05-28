package edu.ecnu.touchstone.datagenerator;

import org.apache.log4j.Logger;

import edu.ecnu.touchstone.pretreatment.TableGeneTemplate;
import edu.ecnu.touchstone.run.Touchstone;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class DataGeneratorServerHandler extends ChannelInboundHandlerAdapter {

	private Logger logger = null;

	public DataGeneratorServerHandler() {
		logger = Logger.getLogger(Touchstone.class);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		TableGeneTemplate template = (TableGeneTemplate)msg;
		DataGenerator.addTemplate(template);

		logger.info("\n\tData generator has recieved a table generation template where the table name is " + 
				template.getTableName() + "!");
		String response = "It's the response of the data generator: The table name of received template is " + 
				template.getTableName() + "!";
		ctx.writeAndFlush(response);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}
}
