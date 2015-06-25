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

package com.dataArtisans.flinkCascading.planning.rules;

import cascading.flow.FlowElement;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.flow.planner.iso.expression.TypeExpression;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.flow.planner.iso.transformer.InsertionGraphTransformer;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.transformer.RuleInsertionTransformer;
import cascading.pipe.GroupBy;

import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

/**
 * Injects a Boundary before a Merge in order to split of the Merge as a separate node.
 */
public class MergeBeforeMergingGroupByTransformer extends RuleInsertionTransformer
{
	public MergeBeforeMergingGroupByTransformer() {
		super(
				BalanceAssembly,
				new MergingGroupByMatcher(),
				MergeElementFactory.MERGE_FACTORY,
				InsertionGraphTransformer.Insertion.Before
		);
	}

	public static class MergingGroupByMatcher extends RuleExpression
	{
		public MergingGroupByMatcher()
		{
			super( new MergingGroupByGraph() );
		}
	}

	public static class MergingGroupByGraph extends ExpressionGraph {

		public MergingGroupByGraph() {

			super(SearchOrder.ReverseTopological);

			arc(
					new FlowElementExpression(FlowElement.class),
					ScopeExpression.ALL,
					new TypeExpression(ElementCapture.Primary, GroupBy.class, TypeExpression.Topo.Splice)
			);

		}
	}

}
