/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataArtisans.flinkCascading.planning;

import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.FlowStepJob;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.Extent;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.management.state.ClientState;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.Joiner;
import cascading.tap.MultiSourceTap;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.dataArtisans.flinkCascading.exec.operators.CoGroupReducer;
import com.dataArtisans.flinkCascading.exec.operators.FileTapInputFormat;
import com.dataArtisans.flinkCascading.exec.operators.FileTapOutputFormat;
import com.dataArtisans.flinkCascading.exec.operators.ReducerJoinKeyExtractor;
import com.dataArtisans.flinkCascading.exec.operators.InnerJoiner;
import com.dataArtisans.flinkCascading.exec.operators.Reducer;
import com.dataArtisans.flinkCascading.exec.operators.Mapper;
import com.dataArtisans.flinkCascading.exec.operators.ProjectionMapper;
import com.dataArtisans.flinkCascading.types.tuple.TupleTypeInfo;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.SortedGrouping;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class FlinkFlowStep extends BaseFlowStep<Configuration> {

	private ExecutionEnvironment env;

	public FlinkFlowStep(ExecutionEnvironment env, ElementGraph elementGraph, FlowNodeGraph flowNodeGraph) {
		super( elementGraph, flowNodeGraph );
		this.env = env;
	}

	// Configures the MapReduce program for this step
	public Configuration createInitializedConfig( FlowProcess<Configuration> flowProcess, Configuration parentConfig ) {

		this.buildFlinkProgram();

		// TODO
		return null;
	}

	protected FlowStepJob<Configuration> createFlowStepJob( ClientState clientState, FlowProcess<Configuration> flowProcess, Configuration initializedStepConfig )
	{
		try {
			return new FlinkFlowStepJob(clientState, this, initializedStepConfig);
		}
		catch(NoClassDefFoundError error) {
			PlatformInfo platformInfo = HadoopUtil.getPlatformInfo();

//			String message = "unable to load platform specific class, please verify Hadoop cluster version: '%s', matches the Hadoop platform build dependency and associated FlowConnector, cascading-hadoop or cascading-hadoop2-mr1";
			String message = "Error"; // TODO

			logError( String.format( message, platformInfo.toString() ), error );

			throw error;
		}
	}

	/**
	 * Method clean removes any temporary files used by this FlowStep instance. It will log any IOExceptions thrown.
	 *
	 * @param config of type JobConf
	 */
	public void clean( Configuration config ) {

		// TODO: Do some clean-up. Check HadoopFlowStep for details.
	}

	public ExecutionEnvironment getExecutionEnvironment() {
		return this.env;
	}

	public JavaPlan getFlinkPlan() {
		return this.env.createProgramPlan();
	}

	private void printFlowStep() {
		Iterator<FlowNode> iterator = getFlowNodeGraph().getTopologicalIterator();

		System.out.println("Step Cnt: "+getFlowNodeGraph().vertexSet().size());
		System.out.println("Edge Cnt: "+getFlowNodeGraph().edgeSet().size());
		System.out.println("Src Set: "+getFlowNodeGraph().getSourceElements());
		System.out.println("Snk Set: "+getFlowNodeGraph().getSinkElements());
		System.out.println("##############");

		while(iterator.hasNext()) {

			FlowNode next = iterator.next();

			System.out.println("Node cnt: "+next.getElementGraph().vertexSet().size());
			System.out.println("Edge cnt: "+next.getElementGraph().edgeSet().size());

			System.out.println("Nodes: "+next.getElementGraph().vertexSet());

			System.out.println("-----------");
		}


	}

	public void buildFlinkProgram() {

		env.setParallelism(1); // TODO: set for tests that count groups

		printFlowStep();

		FlowNodeGraph flowNodeGraph = getFlowNodeGraph();
		Iterator<FlowNode> iterator = flowNodeGraph.getTopologicalIterator(); // TODO: topologicalIterator is non-deterministically broken!!!

//		Map<FlowElement, DataSet<Tuple>> flinkFlows = new HashMap<FlowElement, DataSet<Tuple>>();
		Map<FlowElement, List<DataSet<Tuple>>> flinkMemo = new HashMap<FlowElement, List<DataSet<Tuple>>>();

		while(iterator.hasNext()) {
			FlowNode node = iterator.next();

			Set<FlowElement> sources = getSources(node);
			int numNodes = node.getElementGraph().vertexSet().size() - 2; // don't count head & tail

			if(sources.size() == 1) {

				// single input node: Map, Reduce, Source, Sink

				FlowElement source = getSource(node);
				Set<FlowElement> sinks = getSinks(node);

				// SOURCE
				if (source instanceof Tap
						&& ((Tap) source).isSource()) {

					DataSet<Tuple> sourceFlow = translateSource(node, env);
					for(FlowElement sink : sinks) {
						flinkMemo.put(sink, Collections.singletonList(sourceFlow));
					}
				}
				// SINK
				else if (source instanceof Boundary
						&& sinks.size() == 1
						&& sinks.iterator().next() instanceof Tap) {

					DataSet<Tuple> input = flinkMemo.get(source).get(0);
					translateSink(input, node);
				}
				// SPLIT (Single boundary source, multiple sinks & no intermediate nodes)
				else if (source instanceof Boundary
						&& sinks.size() > 1
						// only sinks + source
						&& numNodes == sinks.size() + 1 ) {

					// just forward
					for(FlowElement sink : sinks) {
						flinkMemo.put(sink, flinkMemo.get(source));
					}

				}
				// EMPTY NODE (Single boundary source, single sink & no intermediate nodes)
				else if (source instanceof Boundary &&
						sinks.size() == 1 &&
						numNodes == 2) {
					for(FlowElement sink : sinks) {
						flinkMemo.put(sink, flinkMemo.get(source));
					}
				}
				// REDUCE (Single groupBy source)
				else if (source instanceof GroupBy) {

					DataSet<Tuple> input = flinkMemo.get(source).get(0);
					DataSet<Tuple> grouped = translateReduce(input, node);
					for(FlowElement sink : sinks) {
						flinkMemo.put(sink, Collections.singletonList(grouped));
					}
				}
				// MAP (Single boundary source)
				else if (source instanceof Boundary) {

					DataSet<Tuple> input = flinkMemo.get(source).get(0);
					DataSet<Tuple> mapped = translateMap(input, node);
					for(FlowElement sink : sinks) {
						flinkMemo.put(sink, Collections.singletonList(mapped));
					}
				}
				// CoGroup (Single CoGroup source)
				else if (source instanceof CoGroup) {

					List<DataSet<Tuple>> inputs = flinkMemo.get(source);
					DataSet<Tuple> coGrouped = translateCoGroup(inputs, node);
					for(FlowElement sink : sinks) {
						flinkMemo.put(sink, Collections.singletonList(coGrouped));
					}

				}
				else {
					throw new RuntimeException("Could not translate this node: "+node.getElementGraph().vertexSet());
				}

			}
			else {

				// multi input node: Merge, (CoGroup, Join)

				boolean allSourcesBoundaries = true;
				for(FlowElement source : sources) {
					if(!(source instanceof Boundary)) {
						allSourcesBoundaries = false;
						break;
					}
				}
				Set<FlowElement> nodeElements = node.getElementGraph().vertexSet();
				Set<FlowElement> innerElements = new HashSet<FlowElement>(nodeElements);
				innerElements.remove(sources);
				innerElements.remove(sinks);

				Set<FlowElement> sinks = getSinks(node);
				// MERGE
				if(allSourcesBoundaries &&
						sinks.size() == 1 &&
						sinks.iterator().next() instanceof Boundary &&
						// only sources + sink + one more node (Merge) + head + tail
						node.getElementGraph().vertexSet().size() == sources.size() + 4) {

					DataSet<Tuple> unioned = translateMerge(flinkMemo, node);
					for(FlowElement sink : sinks) {
						flinkMemo.put(sink, Collections.singletonList(unioned));
					}
				}
				// Input of CoGroup or HashJoin
				else if(allSourcesBoundaries &&
						sinks.size() == 1 &&
						// check that only sources + sink + head + tail are in this node
						node.getElementGraph().vertexSet().size() == sources.size() + 1 + 2 &&
						(
								sinks.iterator().next() instanceof CoGroup ||
								sinks.iterator().next() instanceof HashJoin
						)) {

					Splice splice = (Splice)sinks.iterator().next();

					// register input of CoGroup or HashJoin
					List<DataSet<Tuple>> flinkSpliceInputs = new ArrayList<DataSet<Tuple>>(sources.size());
					ElementGraph eg= node.getElementGraph();
					for(Pipe spliceInput : splice.getPrevious()) {

						String inputName = spliceInput.getName();
						boolean found = false;
						for(FlowElement nodeSource : sources) {
							if(eg.getEdge(nodeSource, splice).getName().equals(inputName)) {
								flinkSpliceInputs.add(flinkMemo.get(nodeSource).get(0));
								found = true;
								break;
							}
						}
						if(!found) {
							throw new RuntimeException("CoGroup input was not found");
						}
					}
					flinkMemo.put(splice, flinkSpliceInputs);

				}
				else {
					throw new UnsupportedOperationException("No multi-input nodes other than Merge supported right now.");
				}


			}

		}

	}

	private DataSet<Tuple> translateSource(FlowNode node, ExecutionEnvironment env) {

		FlowElement source = getSource(node);

		// add data source to Flink program
		if(source instanceof FileTap) {
			return this.translateFileTapSource((FileTap)source, env);
		}
		else if(source instanceof MultiSourceTap) {
			return this.translateMultiSourceTap((MultiSourceTap)source, env);
		}
		else {
			throw new RuntimeException("Unsupported tap type encountered"); // TODO
		}

	}

	private DataSet<Tuple> translateFileTapSource(FileTap tap, ExecutionEnvironment env) {

		Properties conf = new Properties();
		tap.getScheme().sourceConfInit(null, tap, conf);

		DataSet<Tuple> src = env
				.createInput(new FileTapInputFormat(tap, conf), new TupleTypeInfo(tap.getSourceFields()))
				.name(tap.getIdentifier())
				.setParallelism(1);

		return src;
	}

	private DataSet<Tuple> translateMultiSourceTap(MultiSourceTap tap, ExecutionEnvironment env) {

		Iterator<Tap> childTaps = ((MultiSourceTap)tap).getChildTaps();

		DataSet cur = null;
		while(childTaps.hasNext()) {
			Tap childTap = childTaps.next();
			DataSet source;

			if(childTap instanceof FileTap) {
				source = translateFileTapSource((FileTap)childTap, env);
			}
			else {
				throw new RuntimeException("Tap type "+tap.getClass().getCanonicalName()+" not supported yet.");
			}

			if(cur == null) {
				cur = source;
			}
			else {
				cur = cur.union(source);
			}
		}

		return cur;
	}

	private void translateSink(DataSet<Tuple> input, FlowNode node) {

		FlowElement sink = getSink(node);

		if(!(sink instanceof Tap)) {
			throw new IllegalArgumentException("FlowNode is not a sink");
		}

		Tap sinkTap = (Tap)sink;

		Fields tapFields = sinkTap.getSinkFields();
		// check that no projection is necessary
		if(!tapFields.isAll()) {

			Scope inScope = getInScope(node);
			Fields tailFields = inScope.getIncomingTapFields();

			// check if we need to project
			if(!tapFields.equalsFields(tailFields)) {
				// add projection mapper
				input = input
						.map(new ProjectionMapper(tailFields, tapFields))
						.returns(new TupleTypeInfo(tapFields));
			}
		}

		if(sinkTap instanceof FileTap) {
			translateFileSinkTap(input, (FileTap) sinkTap);
		}
		else {
			throw new UnsupportedOperationException("Only file taps as sinks suppported right now.");
		}

	}

	private void translateFileSinkTap(DataSet<Tuple> input, FileTap fileSink) {

		Properties props = new Properties();
		input
				.output(new FileTapOutputFormat(fileSink, fileSink.getSinkFields(), props))
				.setParallelism(1);
	}

	private DataSet<Tuple> translateMap(DataSet<Tuple> input, FlowNode node) {

		Scope outScope = getFirstOutScope(node);

		return input
				.mapPartition(new Mapper(node))
				.withParameters(this.getConfig())
				.returns(new TupleTypeInfo(outScope.getOutValuesFields()));

	}

	private DataSet<Tuple> translateReduce(DataSet<Tuple> input, FlowNode node) {

		GroupBy groupBy = (GroupBy)getSource(node);

		Scope inScope = getInScope(node);
		Scope outScope = getOutScope(node);

		if(groupBy.getKeySelectors().size() != 1) {
			throw new RuntimeException("Currently only groupBy with single input supported");
		}

		// get grouping keys
		Fields keyFields = groupBy.getKeySelectors().get(inScope.getName());
		if(keyFields == null) {
			throw new RuntimeException("No valid key fields found for GroupBy");
		}
		String[] groupKeys = registerKeyFields(input, keyFields);

		// get group sorting keys
		Fields sortKeyFields = groupBy.getSortingSelectors().get(inScope.getName());
		String[] sortKeys = null;
		if(sortKeyFields != null) {
			sortKeys = registerKeyFields(input, sortKeyFields);
		}

		// get output fields for type info
		Fields outFields;
		if(outScope.isEvery()) {
			outFields = outScope.getOutGroupingFields();
		}
		else {
			outFields = outScope.getOutValuesFields();
		}

		// Reduce without group sorting

		if(sortKeys == null) {

			return input
					.groupBy(groupKeys)
					.reduceGroup(new Reducer(node))
					.withParameters(this.getConfig())
					.returns(new TupleTypeInfo(outFields));
		}
		// Reduce with group sorting
		else {

			Order sortOrder;
			if(groupBy.isSortReversed()) {
				sortOrder = Order.DESCENDING;
			}
			else {
				sortOrder = Order.ASCENDING;
			}

			SortedGrouping<Tuple> grouping = input
					.groupBy(groupKeys)
					.sortGroup(sortKeys[0], sortOrder);

			for(int i=1; i<sortKeys.length; i++) {
				grouping = grouping.sortGroup(sortKeys[i], sortOrder);
			}

			return grouping
					.reduceGroup(new Reducer(node))
					.withParameters(this.getConfig())
					.returns(new TupleTypeInfo(outFields));
		}

	}

	private DataSet<Tuple> translateMerge(Map<FlowElement, List<DataSet<Tuple>>> flinkFlows, FlowNode node) {

		// check if remaining node is a Merge
		Set<FlowElement> elements = new HashSet(node.getElementGraph().vertexSet());
		elements.removeAll(getSources(node));
		elements.remove(getSink(node));

		for(FlowElement v : elements) {
			if(!(v instanceof Merge || v instanceof Extent)) {
				throw new RuntimeException("Unexpected non-merge element found.");
			}
		}

		// this node is just a merge wrapped in boundaries.
		// translate it to a Flink union

		Set<FlowElement> sources = getSources(node);

		DataSet<Tuple> unioned = null;
		for(FlowElement source : sources) {
			if(unioned == null) {
				unioned = flinkFlows.get(source).get(0);
			}
			else {
				unioned = unioned.union(flinkFlows.get(source).get(0));
			}
		}
		return unioned;
	}

	private DataSet<Tuple> translateCoGroup(List<DataSet<Tuple>> inputs, FlowNode node) {

		CoGroup coGroup = (CoGroup)node.getSourceElements().iterator().next();

		return translateGroupByCoGroup(inputs, node);
		/*

		Joiner joiner = coGroup.getJoiner();
		if(joiner instanceof InnerJoin) {
			// handle inner join
			return this.translateInnerCoGroup(inputs, node);
		}
		else if(joiner instanceof LeftJoin) {
			// TODO handle left outer join
			throw new UnsupportedOperationException("Left outer join not supported yet");
		}
		else if(joiner instanceof RightJoin) {
			// TODO handle right outer join
			throw new UnsupportedOperationException("Right outer join not supported yet");
		}
		else if(joiner instanceof OuterJoin) {
			// TODO handle full outer join
			throw new UnsupportedOperationException("Full outer join not supported yet");
		}
		else if(joiner instanceof MixedJoin) {
			// TODO handle mixed join
			throw new UnsupportedOperationException("Mixed join not supported yet");
		}
		else if(joiner instanceof BufferJoin) {
			// TODO hanlde buffer join
			// translate to GroupBy
			throw new UnsupportedOperationException("Buffer join not supported yet");
		}
		else {
			// TODO handle user-defined join
			throw new UnsupportedOperationException("User-defined join not supported yet");
		}
		*/
	}

	private DataSet<Tuple> translateGroupByCoGroup(List<DataSet<Tuple>> inputs, FlowNode node) {

		CoGroup coGroup = (CoGroup) node.getSourceElements().iterator().next();

		// prepare inputs: (extract keys and assign input id)
		DataSet<Tuple3<Tuple, Integer, Tuple>> groupByInput = null;

		Scope outScope = getOutScope(node);
		List<Scope> inScopes = getInputScopes(node, coGroup);
		TypeInformation<Tuple3<Tuple, Integer, Tuple>> keyedType = null;

		Fields outFields;
		if(outScope.isEvery()) {
			outFields = outScope.getOutGroupingFields();
		}
		else {
			outFields = outScope.getOutValuesFields();
		}

		for(int i=0; i<inputs.size(); i++) {

			// get Flink DataSet
			DataSet<Tuple> input = inputs.get(i);
			// get input scope
			Scope inputScope = inScopes.get(i);

			// get keys
			Fields inputFields = ((TupleTypeInfo)input.getType()).getFields();
			Fields joinKeyFields = coGroup.getKeySelectors().get(inputScope.getName());
			int[] keyPos = inputFields.getPos(joinKeyFields);

			if(joinKeyFields.isNone()) {
				// set default key
				joinKeyFields = new Fields("defaultKey");
			}

			if(keyedType == null) {
				keyedType = new org.apache.flink.api.java.typeutils.TupleTypeInfo<Tuple3<Tuple, Integer, Tuple>>(
						new TupleTypeInfo(joinKeyFields),
						BasicTypeInfo.INT_TYPE_INFO,
						new TupleTypeInfo(inputFields)
				);
			}

			// add mapper
			DataSet<Tuple3<Tuple, Integer, Tuple>> keyedInput = input
					.map(new ReducerJoinKeyExtractor(i, keyPos))
					.returns(keyedType);

			// add to groupByInput
			if(groupByInput == null) {
				groupByInput = keyedInput;
			}
			else {
				groupByInput = groupByInput
						.union(keyedInput);
			}
		}

		return groupByInput
				.groupBy("f0.*")
				.sortGroup(1, Order.DESCENDING)
				.reduceGroup(new CoGroupReducer(node))
				.withParameters(getConfig())
				.returns(new TupleTypeInfo(outFields));

		// group by input on join key, secondary sort on input id

	}


	private DataSet<Tuple> translateInnerCoGroup(List<DataSet<Tuple>> inputs, FlowNode node) {

		CoGroup coGroup = (CoGroup) node.getSourceElements().iterator().next();
		Joiner joiner = coGroup.getJoiner();
		if (!(joiner instanceof InnerJoin)) {
			throw new IllegalArgumentException("CoGroup must have InnerJoiner");
		}
		if (coGroup.isSelfJoin()) {
			throw new UnsupportedOperationException("Self-join not supported yet");
		}
		if (inputs.size() > 2) {
			throw new UnsupportedOperationException("Only binary CoGroups supported yet");
		}

		DataSet<Tuple> joined = null;
		Fields resultFields = new Fields();
		String[] firstInputJoinKeys = null;

		// get result fields for each input
		List<Scope> inScopes = getInputScopes(node, coGroup);
		List<Fields> resultFieldsByInput = getResultFieldsByInput(coGroup, inScopes);

		// for each input
		for (int i = 0; i < inputs.size(); i++) {

			// get Flink DataSet
			DataSet<Tuple> input = inputs.get(i);
			// get input scope
			Scope inputScope = inScopes.get(i);

			// get join keys
			Fields joinKeyFields = coGroup.getKeySelectors().get(inputScope.getName());
			String[] joinKeys = registerKeyFields(input, joinKeyFields);

			resultFields = resultFields.append(resultFieldsByInput.get(i));

			// first input
			if (joined == null) {

				joined = input;
				firstInputJoinKeys = joinKeys;

			// other inputs
			} else {

				joined = joined.join(input, JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE)
						.where(firstInputJoinKeys).equalTo(joinKeys)
						.with(new InnerJoiner())
						.returns(new TupleTypeInfo(resultFields))
//						.withForwardedFieldsFirst(leftJoinKeys) // TODO
//						.withForwardedFieldsSecond(joinKeys) // TODO
						.withParameters(this.getConfig());

				// TODO: update firstInputJoinKeys, update leftJoinKeys

			}
		}
		return joined;

	}

	private List<Scope> getInputScopes(FlowNode node, Splice splice) {

		Pipe[] inputs = splice.getPrevious();
		List<Scope> inScopes = new ArrayList<Scope>(inputs.length);
		for(Pipe input : inputs) {
			boolean found = false;
			for (Scope inScope : node.getPreviousScopes(splice)) {
				if(inScope.getName().equals(input.getName())) {
					inScopes.add(inScope);
					found = true;
					break;
				}
			}
			if(!found) {
				throw new RuntimeException("Input scope was not found");
			}
		}

		return inScopes;
	}

	private List<Fields> getResultFieldsByInput(Splice splice, List<Scope> inScopes) {

		List<Fields> resultFieldsByInput = new ArrayList<Fields>();

		if(splice.getJoinDeclaredFields() == null) {
			for(int i=0; i<inScopes.size(); i++) {
				Fields resultFields = inScopes.get(i).getOutValuesFields();

				resultFieldsByInput.add(resultFields);
			}
		}
		else {
			int cnt = 0;
			Fields declaredFields = splice.getJoinDeclaredFields();
			for(int i=0; i<inScopes.size(); i++) {
				Fields inputFields = inScopes.get(i).getOutValuesFields();

				Fields resultFields = new Fields();

				for(int j=0; j<inputFields.size(); j++) {
					Comparable name = declaredFields.get(cnt++);
					Type type = inputFields.getType(j);

					if(type != null) {
						resultFields = resultFields.append(new Fields(name, type));
					}
					else {
						resultFields = resultFields.append(new Fields(name));
					}
				}

				resultFieldsByInput.add(resultFields);
			}
		}

		return resultFieldsByInput;
	}

//	private void setCustomComparators(Fields keys, DataSet<Tuple> input) {
//
//		if(fields == null) {
//			throw new IllegalArgumentException("Fields may not be null");
//		}
//		else if(comparators == null) {
//			throw new IllegalArgumentException("Comparators may not be null");
//		}
//		else if(fields.length != comparators.length) {
//			throw new IllegalArgumentException("Fields and Comparators must have same length");
//		}
//
//		// get type info of input
//		CascadingTupleTypeInfo tupleType = (CascadingTupleTypeInfo)input.getType();
//
//		for(int i=0; i<comparators.length; i++) {
//			if(comparators[i] != null) {
//				tupleType.setFieldComparator(fields[i], comparators[i]);
//			}
//		}
//	}

	private Set<FlowElement> getSources(FlowNode node) {
		return node.getSourceElements();
	}

	private Set<FlowElement> getSinks(FlowNode node) {
		return node.getSinkElements();
	}

	private FlowElement getSource(FlowNode node) {
		Set<FlowElement> nodeSources = node.getSourceElements();
		if(nodeSources.size() != 1) {
			throw new RuntimeException("Only nodes with one input supported right now");
		}
		return nodeSources.iterator().next();
	}

	private FlowElement getSink(FlowNode node) {
		Set<FlowElement> nodeSinks = node.getSinkElements();
		if(nodeSinks.size() != 1) {
			throw new RuntimeException("Only nodes with one output supported right now");
		}
		return nodeSinks.iterator().next();
	}

	private Scope getInScope(FlowNode node) {

		FlowElement source = getSource(node);

		Collection<Scope> inScopes = (Collection<Scope>) node.getPreviousScopes(source);
		if(inScopes.size() != 1) {
			throw new RuntimeException("Only one incoming scope for last node of mapper allowed");
		}
		return inScopes.iterator().next();
	}

	private Scope getOutScope(FlowNode node) {

		FlowElement sink = getSink(node);

		Collection<Scope> outScopes = (Collection<Scope>) node.getPreviousScopes(sink);
		if(outScopes.size() != 1) {
			throw new RuntimeException("Only one incoming scope for last node of mapper allowed");
		}
		return outScopes.iterator().next();
	}

	private Scope getFirstOutScope(FlowNode node) {

		FlowElement firstSink = getSinks(node).iterator().next();

		Collection<Scope> outScopes = (Collection<Scope>) node.getPreviousScopes(firstSink);
		if(outScopes.size() != 1) {
			throw new RuntimeException("Only one incoming scope for last node of mapper allowed");
		}
		return outScopes.iterator().next();

	}

	private String[] registerKeyFields(DataSet<Tuple> input, Fields keyFields) {
		return ((TupleTypeInfo)input.getType()).registerKeyFields(keyFields);
	}

}
