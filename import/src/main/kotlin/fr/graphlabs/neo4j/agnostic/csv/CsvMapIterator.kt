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
package fr.graphlabs.neo4j.agnostic.csv

import org.supercsv.io.CsvMapReader
import org.supercsv.prefs.CsvPreference
import java.io.Reader

class CsvMapIterator(private val columns: Array<String?>,
                     reader: Reader,
                     skipHeader: Boolean = true,
                     prefs: CsvPreference = CsvPreference.STANDARD_PREFERENCE) : Iterator<Map<String, String>> {

    private val csvReader = CsvMapReader(reader, prefs)
    init {
        if (skipHeader) {
            csvReader.getHeader(true)
        }
    }

    private var next: Map<String, String>? = null

    override fun next(): Map<String, String> {
        if (next != null || hasNext()) {
            val result = next!!
            next = null
            return result
        }
        throw NoSuchElementException()
    }

    override fun hasNext(): Boolean {
        if (next != null) {
            return true
        }
        next = csvReader.read(*columns)
        return next != null
    }

}
