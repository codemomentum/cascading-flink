/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading.planner;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.conf.Configuration;

import carbonite.ClojureMapSerializer;
import carbonite.ClojureReaderSerializer;
import carbonite.ClojureSeqSerializer;
import carbonite.ClojureSetSerializer;
import carbonite.ClojureVecSerializer;
import carbonite.RatioSerializer;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.flow.planner.BaseFlowStepFactory;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlannerInfo;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.FlowStepFactory;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.tap.Tap;
import clojure.lang.ArraySeq;
import clojure.lang.Cons;
import clojure.lang.IteratorSeq;
import clojure.lang.Keyword;
import clojure.lang.LazySeq;
import clojure.lang.MapEntry;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentHashSet;
import clojure.lang.PersistentList;
import clojure.lang.PersistentStructMap;
import clojure.lang.PersistentVector;
import clojure.lang.Ratio;
import clojure.lang.Symbol;

public class FlinkPlanner extends FlowPlanner<FlinkFlow, Configuration> {

	private Configuration defaultConfig;

	private List<String> classPath;

	private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	public FlinkPlanner(List<String> classPath) {
	super();
	this.classPath = classPath;

		env.getConfig().disableSysoutLogging();

	if (env.getParallelism() <= 0) {
		// load the default parallelism from config
		GlobalConfiguration.loadConfiguration(new File(CliFrontend.getConfigurationDirectoryFromEnv()).getAbsolutePath());
		org.apache.flink.configuration.Configuration configuration = GlobalConfiguration.getConfiguration();
		int parallelism = configuration.getInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, -1);
		if (parallelism <= 0) {
		throw new RuntimeException("Please set the default parallelism via the -p command-line flag");
		} else {
		env.setParallelism(parallelism);
		}
	}

	//env.addDefaultKryoSerializer(PersistentVector.class, ClojureVecSerializer.class);

			ClojureVecSerializer vecSerializer = new ClojureVecSerializer();
			ClojureSetSerializer setSerializer = new ClojureSetSerializer();

			env.registerTypeWithKryoSerializer(PersistentVector.class, vecSerializer);
			env.registerTypeWithKryoSerializer(PersistentHashSet.class, setSerializer);
			env.registerTypeWithKryoSerializer(MapEntry.class, vecSerializer);

			ClojureSeqSerializer seqSerializer = new ClojureSeqSerializer();
			env.registerTypeWithKryoSerializer(MapEntry.class, seqSerializer);
			env.registerTypeWithKryoSerializer(Cons.class, seqSerializer);
			env.registerTypeWithKryoSerializer(PersistentList.class, seqSerializer);
			env.registerTypeWithKryoSerializer(LazySeq.class, seqSerializer);
			env.registerTypeWithKryoSerializer(IteratorSeq.class, seqSerializer);
			env.registerTypeWithKryoSerializer(ArraySeq.class, seqSerializer);
			env.registerTypeWithKryoSerializer(PersistentVector.ChunkedSeq.class, seqSerializer);
			//env.registerTypeWithKryoSerializer(PersistentList.EmptyList.class, seqSerializer);

			ClojureMapSerializer mapSerializer = new ClojureMapSerializer();
			env.registerTypeWithKryoSerializer(PersistentArrayMap.class, mapSerializer);
			env.registerTypeWithKryoSerializer(PersistentHashMap.class, mapSerializer);
			env.registerTypeWithKryoSerializer(PersistentStructMap.class, mapSerializer);

			env.registerTypeWithKryoSerializer(Keyword.class, new ClojureReaderSerializer());
			env.registerTypeWithKryoSerializer(Symbol.class, new ClojureReaderSerializer());
			env.registerTypeWithKryoSerializer(Ratio.class, new RatioSerializer());
//			env.registerTypeWithKryoSerializer(Var.class, new PrintDupSerializer());



	}

	@Override
	public Configuration getDefaultConfig() {
	return defaultConfig;
	}

	@Override
	public PlannerInfo getPlannerInfo(String registryName) {
	return new PlannerInfo(getClass().getSimpleName(), "Apache Flink", registryName);
	}

	@Override
	public PlatformInfo getPlatformInfo() {
	return new PlatformInfo("Apache Flink", "data Artisans GmbH", "0.1");
	}

	@Override
	public void initialize(	FlowConnector flowConnector,
							Map<Object, Object> properties) {

	super.initialize(flowConnector, properties);
	defaultConfig = createConfiguration(properties);
	}

	@Override
	public void configRuleRegistryDefaults(RuleRegistry ruleRegistry) {
	super.configRuleRegistryDefaults(ruleRegistry);
	}

	@Override
	protected FlinkFlow createFlow(FlowDef flowDef) {
	return new FlinkFlow(getPlatformInfo(), flowDef, getDefaultProperties(), getDefaultConfig());
	}

	@Override
	public FlowStepFactory<Configuration> getFlowStepFactory() {

	return new BaseFlowStepFactory<Configuration>(getFlowNodeFactory()) {
		@Override
		public FlowStep<Configuration> createFlowStep(	ElementGraph stepElementGraph,
														FlowNodeGraph flowNodeGraph) {
		return new FlinkFlowStep(env, stepElementGraph, flowNodeGraph, classPath);
		}
	};
	}

	@Override
	protected Tap makeTempTap(	String prefix,
								String name) {
	return null; // not required for Flink
	}

	public static Configuration createConfiguration(Map<Object, Object> properties) {
	Configuration conf = new Configuration();
	copyProperties(conf, properties);
	return conf;
	}

	public static void copyProperties(	Configuration config,
										Map<Object, Object> properties) {
	if (properties instanceof Properties) {
		Properties props = (Properties) properties;
		Set<String> keys = props.stringPropertyNames();

		for (String key : keys) {
		config.set(key, props.getProperty(key));
		}
	} else {
		for (Map.Entry<Object, Object> entry : properties.entrySet()) {
		if (entry.getValue() != null) {
			config.set(entry.getKey().toString(), entry.getValue().toString());
		}
		}
	}
	}

}
