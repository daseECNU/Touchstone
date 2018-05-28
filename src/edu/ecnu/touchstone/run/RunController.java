package edu.ecnu.touchstone.run;

import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import edu.ecnu.touchstone.constraintchain.ConstraintChain;
import edu.ecnu.touchstone.constraintchain.ConstraintChainsReader;
import edu.ecnu.touchstone.controller.Controller;
import edu.ecnu.touchstone.pretreatment.Preprocessor;
import edu.ecnu.touchstone.pretreatment.TableGeneTemplate;
import edu.ecnu.touchstone.queryinstantiation.ComputingThreadPool;
import edu.ecnu.touchstone.queryinstantiation.Parameter;
import edu.ecnu.touchstone.queryinstantiation.QueryInstantiator;
import edu.ecnu.touchstone.schema.SchemaReader;
import edu.ecnu.touchstone.schema.Table;

public class RunController {

	// args[0]: the path of the configuration file
	public static void main(String[] args) {
		Configurations configurations = new Configurations(args[0]);
		
		PropertyConfigurator.configure(configurations.getLog4jConfFile());
		System.setProperty("com.wolfram.jlink.libdir", configurations.getjLinkPath());
		
		SchemaReader schemaReader = new SchemaReader();
		List<Table> tables = schemaReader.read(configurations.getDatabaseSchemaInput());
		
		ConstraintChainsReader constraintChainsReader = new ConstraintChainsReader();
		List<ConstraintChain> constraintChains = constraintChainsReader.
				read(configurations.getCardinalityConstraintsInput());
		
//		NonEquiJoinConstraintsReader nonEquiJoinConstraintsReader = new NonEquiJoinConstraintsReader();
//		List<NonEquiJoinConstraint> nonEquiJoinConstraints = nonEquiJoinConstraintsReader.
//				read(configurations.getNonEquiJoinConstraintsInput());
		
		ComputingThreadPool computingThreadPool = new ComputingThreadPool(
				configurations.getQueryInstantiationThreadNum(), 
				configurations.getParaInstantiationMaxIterations(), 
				configurations.getParaInstantiationRelativeError());
		QueryInstantiator queryInstantiator = new QueryInstantiator(
				tables, constraintChains, null, 
//				tables, constraintChains, nonEquiJoinConstraints, 
				configurations.getQueryInstantiationMaxIterations(), 
				configurations.getQueryInstantiationGlobalRelativeError(), 
				computingThreadPool);
		queryInstantiator.iterate();
		List<Parameter> parameters = queryInstantiator.getParameters();
		
		Preprocessor preprocessor = new Preprocessor(tables, constraintChains, parameters);
		List<String> tablePartialOrder = preprocessor.getPartialOrder();
		Map<String, TableGeneTemplate> tableGeneTemplateMap = preprocessor.getTableGeneTemplates(
				configurations.getShuffleMaxNum(), configurations.getPkvsMaxSize());
		
		Controller controller = new Controller(tablePartialOrder, tableGeneTemplateMap, configurations);
		controller.setUpNetworkThreads();
		controller.geneData();
	}
}
