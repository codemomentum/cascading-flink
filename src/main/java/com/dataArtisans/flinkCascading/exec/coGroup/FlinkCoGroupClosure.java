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

package com.dataArtisans.flinkCascading.exec.coGroup;

import cascading.flow.FlowProcess;
import cascading.pipe.joiner.JoinerClosure;
import cascading.provider.FactoryLoader;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.Tuples;
import cascading.tuple.collect.Spillable;
import cascading.tuple.collect.SpillableTupleList;
import cascading.tuple.collect.TupleCollectionFactory;
import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.hadoop.collect.HadoopSpillableTupleList;
import cascading.tuple.util.TupleViews;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static cascading.tuple.collect.TupleCollectionFactory.TUPLE_COLLECTION_FACTORY;

public class FlinkCoGroupClosure extends JoinerClosure {

	protected Iterator[] values;
	protected Collection<Tuple>[] collections;

	protected final int numSelfJoins;
	private Tuple[] joinedTuplesArray;
	private final Tuple emptyTuple;
	private TupleBuilder joinedBuilder;

	protected Tuple grouping;
	private Tuple joinedTuple = new Tuple(); // is discarded


	private final TupleCollectionFactory<Configuration> tupleCollectionFactory;

	public FlinkCoGroupClosure(FlowProcess flowProcess, int numSelfJoins, Fields[] joinFields, Fields[] valueFields) {
		super(flowProcess, joinFields, valueFields);
		this.numSelfJoins = numSelfJoins;

		this.emptyTuple = Tuple.size( joinFields[0].size() );
		FactoryLoader loader = FactoryLoader.getInstance();

		this.tupleCollectionFactory = loader.loadFactoryFrom( flowProcess, TUPLE_COLLECTION_FACTORY, FlinkTupleCollectionFactory.class );

		initLists();
	}

	@Override
	public int size() {
		return Math.max( joinFields.length, numSelfJoins + 1 );
	}

	@Override
	public Iterator<Tuple> getIterator(int pos) {
		if( pos < 0 || pos >= collections.length ) {
			throw new IllegalArgumentException("invalid group position: " + pos);
		}

		return makeIterator( pos, collections[ pos ].iterator() );
	}

	@Override
	public boolean isEmpty(int pos) {
		return collections[ pos ].isEmpty();
	}

	@Override
	public Tuple getGroupTuple(Tuple keysTuple) {
		Tuples.asModifiable(joinedTuple);

		for( int i = 0; i < collections.length; i++ ) {
			joinedTuplesArray[i] = collections[i].isEmpty() ? emptyTuple : keysTuple;
		}

		joinedTuple = joinedBuilder.makeResult( joinedTuplesArray );

		return joinedTuple;
	}

	public Tuple getGrouping() {
		return this.grouping;
	}


	private void initLists() {

		collections = new Collection[ size() ];

		// handle self joins
		if( numSelfJoins != 0 ) {
			Arrays.fill(collections, createTupleCollection(joinFields[0]));
		}
		else {
			collections[ 0 ] = new FalseCollection(); // we iterate this only once per grouping

			for( int i = 1; i < joinFields.length; i++ ) {
				collections[i] = createTupleCollection(joinFields[i]);
			}
		}

		joinedBuilder = makeJoinedBuilder( joinFields );
		joinedTuplesArray = new Tuple[ collections.length ];
	}

	private Collection<Tuple> createTupleCollection( Fields joinField ) {

		Collection<Tuple> collection = tupleCollectionFactory.create( flowProcess );

		if( collection instanceof Spillable) {
			((Spillable) collection).setSpillListener(new SpillListener(flowProcess, joinField));
		}

		return collection;
	}

	private TupleBuilder makeJoinedBuilder( final Fields[] joinFields )
	{
		final Fields[] fields = isSelfJoin() ? new Fields[ size() ] : joinFields;

		if( isSelfJoin() ) {
			Arrays.fill(fields, 0, fields.length, joinFields[0]);
		}

		return new TupleBuilder()
		{
			Tuple result = TupleViews.createComposite(fields);

			@Override
			public Tuple makeResult( Tuple[] tuples )
			{
				return TupleViews.reset( result, tuples );
			}
		};
	}

	private Iterator<Tuple> makeIterator( final int pos, final Iterator values )
	{
		return new Iterator<Tuple>()
		{
			final int cleanPos = valueFields.length == 1 ? 0 : pos; // support repeated pipes
			cascading.tuple.util.TupleBuilder[] valueBuilder = new cascading.tuple.util.TupleBuilder[ valueFields.length ];

			{
				for( int i = 0; i < valueFields.length; i++ ) {
					valueBuilder[i] = makeBuilder(valueFields[i], joinFields[i]);
				}
			}

			private cascading.tuple.util.TupleBuilder makeBuilder( final Fields valueField, final Fields joinField )
			{
				if( valueField.isUnknown() || joinField.isNone() ) {

					return new cascading.tuple.util.TupleBuilder()
					{
						@Override
						public Tuple makeResult( Tuple valueTuple, Tuple groupTuple )
						{
							valueTuple.set( valueFields[ cleanPos ], joinFields[ cleanPos ], groupTuple );

							return valueTuple;
						}
					};
				}
				else {

					return new cascading.tuple.util.TupleBuilder() {
						Tuple result = TupleViews.createOverride(valueField, joinField);

						@Override
						public Tuple makeResult(Tuple valueTuple, Tuple groupTuple) {
							return TupleViews.reset(result, valueTuple, groupTuple);
						}
					};
				}
			}

			public boolean hasNext() {
				return values.hasNext();
			}

			public Tuple next() {
				Tuple tuple = (Tuple) values.next();

				return valueBuilder[ cleanPos ].makeResult( tuple, grouping );
			}

			public void remove() {
				throw new UnsupportedOperationException( "remove not supported" );
			}
		};
	}

	public void reset(Iterator<Tuple3<Tuple, Integer, Tuple>>... values ) {

		this.values = values;

		clearGroups();

		if( collections[ 0 ] instanceof FalseCollection ) { // force reset on FalseCollection
			((FalseCollection) collections[0]).reset(null);
		}

		while( values[ 0 ].hasNext() ) {

			Tuple3<Tuple, Integer, Tuple> v = values[0].next();

			if(!joinFields[0].isNone()) {
				this.grouping = v.f0;
			}
			else {
				// key was default key for none-join
				this.grouping = new Tuple();
			}
			Tuple current = v.f2;
			int pos = v.f1;

			// if this is the first (lhs) co-group, just use values iterator
			// we are guaranteed all the remainder tuples in the iterator are from pos == 0
			if( numSelfJoins == 0 && pos == 0 ) {
				( (FalseCollection) collections[ 0 ] ).reset( createIterator( current, new FlinkUnwrappingIterator<Tuple, Integer>(values[0])) );
				break;
			}

			collections[ pos ].add( current ); // get the value tuple for this cogroup
		}
	}

	protected void clearGroups() {
		for( Collection<Tuple> collection : collections ) {
			collection.clear();

			if( collection instanceof Spillable ) {
				((Spillable) collection).setGrouping(grouping);
			}
		}
	}

	public Iterator<Tuple> createIterator( final Tuple current, final Iterator<Tuple> values ) {
		return new Iterator<Tuple>()
		{
			Tuple value = current;

			@Override
			public boolean hasNext() {
				return value != null;
			}

			@Override
			public Tuple next() {

				if( value == null && !values.hasNext() ) {
					throw new NoSuchElementException();
				}

				Tuple result = value;

				if( values.hasNext() ) {
					value = values.next();
				}
				else {
					value = null;
				}

				return result;
			}

			@Override
			public void remove() {
				// unsupported
			}
		};
	}


	public enum Spill
	{
		Num_Spills_Written, Num_Spills_Read, Num_Tuples_Spilled, Duration_Millis_Written
	}

	static interface TupleBuilder {
		Tuple makeResult( Tuple[] tuples );
	}

	private class SpillListener implements Spillable.SpillListener {
		private final FlowProcess flowProcess;
		private final Fields joinField;

		public SpillListener( FlowProcess flowProcess, Fields joinField ) {
			this.flowProcess = flowProcess;
			this.joinField = joinField;
		}

		@Override
		public void notifyWriteSpillBegin( Spillable spillable, int spillSize, String spillReason ) {
			int numFiles = spillable.spillCount();

			if( numFiles % 10 == 0 ) {
//				LOG.info( "spilling group: {}, on grouping: {}, num times: {}, with reason: {}",
//						new Object[]{joinField.printVerbose(), spillable.getGrouping().print(), numFiles + 1, spillReason} );

				Runtime runtime = Runtime.getRuntime();
				long freeMem = runtime.freeMemory() / 1024 / 1024;
				long maxMem = runtime.maxMemory() / 1024 / 1024;
				long totalMem = runtime.totalMemory() / 1024 / 1024;

//				LOG.info( "mem on spill (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem );
			}

//			LOG.info( "spilling {} tuples in list to file number {}", spillSize, numFiles + 1 );

			flowProcess.increment( Spill.Num_Spills_Written, 1 );
			flowProcess.increment( Spill.Num_Tuples_Spilled, spillSize );
		}

		@Override
		public void notifyWriteSpillEnd( SpillableTupleList spillableTupleList, long duration ) {
			flowProcess.increment( Spill.Duration_Millis_Written, duration );
		}

		@Override
		public void notifyReadSpillBegin( Spillable spillable ) {
			flowProcess.increment( Spill.Num_Spills_Read, 1 );
		}
	}

	public static class FlinkTupleCollectionFactory implements TupleCollectionFactory<Configuration> {
		private int spillThreshold;
		private CompressionCodec codec;
		private TupleSerialization tupleSerialization;

		public FlinkTupleCollectionFactory() {
		}

		public void initialize(FlowProcess<? extends Configuration> flowProcess) {
			this.spillThreshold = SpillableTupleList.getThreshold(flowProcess, 10000);
			this.codec = new GzipCodec();
			this.tupleSerialization = new TupleSerialization(flowProcess);
		}

		public Collection<Tuple> create(FlowProcess<? extends Configuration> flowProcess) {

			// TODO: Replace by a FlinkSplillableTupleList ?
			return new HadoopSpillableTupleList(this.spillThreshold, this.tupleSerialization, this.codec);
		}
	}

	private static class FlinkUnwrappingIterator<F1, F2> implements Iterator<Tuple> {

		private Iterator<Tuple3<F1, F2, Tuple>> flinkIterator;


		public FlinkUnwrappingIterator(Iterable<Tuple3<F1, F2, Tuple>> vals) {
			this(vals.iterator());
		}

		public FlinkUnwrappingIterator(Iterator<Tuple3<F1, F2, Tuple>> vals) {
			this.flinkIterator = vals;
		}

		@Override
		public boolean hasNext() {
			return flinkIterator.hasNext();
		}

		@Override
		public Tuple next() {

			return flinkIterator.next().f2;
		}
	}

	private static class FalseCollection implements Collection<Tuple> {

		boolean returnedIterator = false;
		Iterator<Tuple> iterator;

		public void reset(Iterator<Tuple> iterator) {
			this.returnedIterator = false;
			this.iterator = iterator;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return iterator == null || !iterator.hasNext();
		}

		@Override
		public boolean contains(Object o) {
			return false;
		}

		@Override
		public Iterator<Tuple> iterator() {
			if (returnedIterator) {
				throw new IllegalStateException("may not iterate this tuple stream more than once");
			}

			try {
				if (iterator == null) {
					return Collections.emptyIterator();
				}

				return iterator;
			} finally {
				returnedIterator = true;
			}
		}

		@Override
		public Object[] toArray() {
			return new Object[0];
		}

		@Override
		public <T> T[] toArray(T[] a) {
			return null;
		}

		@Override
		public boolean add(Tuple tuple) {
			return false;
		}

		@Override
		public boolean remove(Object o) {
			return false;
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			return false;
		}

		@Override
		public boolean addAll(Collection<? extends Tuple> c) {
			return false;
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			return false;
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			return false;
		}

		@Override
		public void clear() {
			iterator = null;
		}

	}

}
