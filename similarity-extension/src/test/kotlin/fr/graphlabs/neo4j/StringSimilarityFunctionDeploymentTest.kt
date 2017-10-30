/**
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.graphlabs.neo4j

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Percentage
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.neo4j.harness.junit.Neo4jRule
import org.slf4j.bridge.SLF4JBridgeHandler
import java.util.stream.Collectors

class StringSimilarityFunctionDeploymentTest {

    @get:Rule
    val neo4j: Neo4jRule = Neo4jRule().withFunction(StringSimilarityFunction::class.java)

    companion object {
        @JvmStatic
        @BeforeClass
        fun prepareAll() {
            SLF4JBridgeHandler.removeHandlersForRootLogger()
        }
    }

    @Test
    fun `computes similarity`() {
        val similarity = callSimilarityFunction("some loud", "soso bear")

        assertThat(similarity).isCloseTo(2*1.0/12, Percentage.withPercentage(.01))
    }

    private fun callSimilarityFunction(input1: String, input2: String): Double {
        neo4j.graphDatabaseService.execute("RETURN strings.similarity('$input1', '$input2') AS result").use {
            val similarities = it.columnAs<Double>("result").stream().collect(Collectors.toList())
            assertThat(similarities).hasSize(1)
            return similarities.iterator().next()
        }
    }
}
