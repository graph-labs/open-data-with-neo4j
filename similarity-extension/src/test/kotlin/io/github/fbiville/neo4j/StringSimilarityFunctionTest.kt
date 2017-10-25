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
package io.github.fbiville.neo4j

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.data.Percentage
import org.junit.Test

class StringSimilarityFunctionTest {

    private val subject = StringSimilarityFunction()

    @Test
    fun `two equal inputs must have a similarity of 1`() {
        val similarity = subject.computeSimilarity("some input", "some input")

        assertThat(similarity).isCloseTo(1.0, Percentage.withPercentage(.01))
    }

    @Test
    fun `two inputs differing only by whitespaces should have a similarity of 1`() {
        val similarity = subject.computeSimilarity("some input", "  some   input   ")

        assertThat(similarity).isCloseTo(1.0, Percentage.withPercentage(.01))
    }

    @Test
    fun `two inputs differing only by case should have a similarity of 1`() {
        val similarity = subject.computeSimilarity("some input", "SOme INpuT")

        assertThat(similarity).isCloseTo(1.0, Percentage.withPercentage(.01))
    }

    @Test
    fun `the similarity should be the ratio of the double of matched letter pair count by the total number of pair`() {
        val similarity = subject.computeSimilarity("some loud", "soso bear")

        assertThat(similarity).isCloseTo(2*1.0/12, Percentage.withPercentage(.01))
    }
}