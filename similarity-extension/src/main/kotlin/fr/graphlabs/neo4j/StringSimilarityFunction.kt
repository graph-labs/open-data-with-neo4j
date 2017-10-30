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

import org.neo4j.procedure.Name
import org.neo4j.procedure.UserFunction
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

typealias Bigram = Pair<Char, Char>

class StringSimilarityFunction {

    companion object {
        val totalMatch = 1.0
    }

    @UserFunction(name = "strings.similarity")
    fun computeSimilarity(@Name("input1") input1: String, @Name("input2") input2: String): Double {
        if (input1 == input2) {
            return totalMatch
        }

        val whitespace = Regex("\\s+")
        val words1 = normalizedWords(input1, whitespace)
        val words2 = normalizedWords(input2, whitespace)
        if (words1 == words2) {
            return totalMatch
        }

        val matchCount = AtomicInteger(0)
        val initialPairs1 = allPairs(words1)
        val initialPairs2 = allPairs(words2)
        val pairs2 = initialPairs2.toMutableList()
        initialPairs1.forEach {
            val pair1 = it
            val matchIndex = pairs2.indexOfFirst { it == pair1 }
            if (matchIndex > -1) {
                matchCount.incrementAndGet()
                pairs2.removeAt(matchIndex)
                return@forEach
            }
        }
        return 2.0 * matchCount.get() / (initialPairs1.size + initialPairs2.size)
    }

    private fun normalizedWords(input: String, whitespace: Regex) =
            input.toUpperCase(Locale.FRENCH).split(whitespace).filterNot { it.isBlank() }

    private fun allPairs(input: List<String>): List<Bigram> {
        return input.flatMap { asPair(it) }
    }

    private fun asPair(word: String): Iterable<Bigram> {
        return word.indices
                .takeWhile { it != word.indices.last }
                .map { Pair(word[it], word[it + 1]) }
    }

}
