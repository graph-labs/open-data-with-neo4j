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
package fr.graphlabs.neo4j.agnostic.collections

import org.assertj.core.api.Assertions.assertThat
import org.junit.Assert.fail
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger

class SequenceExtensionsTest {

    @Test
    fun `converts to list of groups without consuming group`() {
        val batchLists = listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).asSequence().batch(2).toList()

        assertThat(batchLists).hasSize(5)
        assertThat(listOf(1, 2)).isEqualTo(batchLists[0].toList())
        assertThat(listOf(3, 4)).isEqualTo(batchLists[1].toList())
        assertThat(listOf(5, 6)).isEqualTo(batchLists[2].toList())
        assertThat(listOf(7, 8)).isEqualTo(batchLists[3].toList())
        assertThat(listOf(9, 10)).isEqualTo(batchLists[4].toList())
    }

    @Test
    fun `converts to list of groups with batch size not multiple of the stream size`() {
        val originalStream = listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

        val batchLists = originalStream.asSequence().batch(3).map { group ->
            group.toList()
        }.toList()

        assertThat(listOf(1, 2, 3)).isEqualTo(batchLists[0])
        assertThat(listOf(4, 5, 6)).isEqualTo(batchLists[1])
        assertThat(listOf(7, 8, 9)).isEqualTo(batchLists[2])
        assertThat(listOf(10)).isEqualTo(batchLists[3])
    }


    @Test
    fun `batches groups of exact size`() {
        val groupCount = AtomicInteger(0)
        val inputItems = listOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
        val collectedItems = ArrayList<Int>()

        inputItems.asSequence().batch(5).forEach { groupStream ->
            groupCount.incrementAndGet()
            groupStream.forEach { item ->
                collectedItems.add(item)
            }
        }

        assertThat(inputItems).isEqualTo(collectedItems)
        assertThat(groupCount.get()).isEqualTo(3)
    }

    @Test
    fun `batches everything into 1 chunk when the batch size is bigger than the actual stream`() {
        val groupCount = AtomicInteger(0)
        val inputItems = listOf(1, 2, 3)
        val collectedItems = ArrayList<Int>()

        inputItems.asSequence().batch(5).forEach { groupStream ->
            groupCount.incrementAndGet()
            groupStream.forEach { item ->
                collectedItems.add(item)
            }
        }

        assertThat(collectedItems).isEqualTo(collectedItems)
        assertThat(groupCount.get()).isEqualTo(1)
    }

    @Test
    fun `batches to chunks of size 1`() {
        val groupCount = AtomicInteger(0)
        val inputItems = listOf(1, 2, 3)
        val collectedItems = ArrayList<Int>()

        inputItems.asSequence().batch(1).forEach { groupStream ->
            groupCount.incrementAndGet()
            groupStream.forEach { item ->
                collectedItems.add(item)
            }
        }

        assertThat(inputItems).isEqualTo(collectedItems)
        assertThat(groupCount.get()).isEqualTo(3)
    }

    @Test
    fun `does not batch when requested batch size is negative`() {
        val inputItems = listOf(1, 2, 3)

        assertThat(inputItems.asSequence().batch(0).toList()).isEmpty()
        assertThat(inputItems.asSequence().batch(-42).toList()).isEmpty()

    }

    @Test
    fun `does not batch when input source is empty`() {
        listOf<Int>().asSequence().batch(1).forEach { _ ->
            fail()
        }
    }

}
