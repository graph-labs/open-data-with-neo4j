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
package fr.graphlabs.neo4j.drugs

import fr.graphlabs.neo4j.*
import org.neo4j.driver.v1.AccessMode
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.StatementResult
import org.supercsv.prefs.CsvPreference
import java.io.Reader
import java.util.*
import java.util.stream.Stream
import kotlin.streams.asSequence

class DrugImporter(boltUri: String, username: String? = null, password: String? = null) {

    private val database =
            if (username == null) {
                GraphDatabase.driver(boltUri)
            } else {
                GraphDatabase.driver(boltUri, AuthTokens.basic(username, password.orEmpty()))
            }

    fun import(reader: Reader, commitPeriod: Int = 500, labNameSimilarity: Double = 0.8) {
        reader.use {
            streamRows(it)
                    .asSequence()
                    .batch(commitPeriod)
                    .forEach {
                        val rows = it
                        database.session(AccessMode.WRITE).use {
                            importDrugGraph(it, rows, labNameSimilarity)
                        }
                    }
        }
        database.session(AccessMode.WRITE).use {
            createIndices(it)
        }
    }

    private fun streamRows(reader: Reader): Stream<Map<String, Any>> {
        val stream = Streams.fromIterator(CsvMapIterator(arrayOf(
                "cisCode", "drugName",
                null, null, null, null, null,
                null, null, null,
                "labNames",
                null
        ), reader, skipHeader = false, prefs = CsvPreference.TAB_PREFERENCE))

        return stream
                .map {
                    val rows = it.toMutableMap()
                    rows.setValuesUpperCase(Locale.FRENCH)
                    val result = rows as MutableMap<String, Any>
                    result["labNames"] = rows["labNames"]!!.split(";").map { it.trim() }.toTypedArray()
                    result
                }
    }

    private fun importDrugGraph(session: Session, rows: List<Map<String, Any>>, labNameSimilarity: Double): StatementResult {
        return session.run("""
            UNWIND {rows} as row
            MERGE (drug:Drug {cisCode: row.cisCode, name: row.drugName})
            WITH drug, row
            UNWIND row.labNames AS labName
            MATCH (lab:Company)
            WITH drug, lab, labName, strings.similarity(labName, lab.name) AS similarity
            WITH drug, CASE WHEN similarity > {threshold} THEN lab ELSE NULL END AS lab, labName
            ORDER BY similarity DESC
            WITH drug, labName, HEAD(COLLECT(lab)) AS lab
            FOREACH (ignored IN CASE WHEN lab IS NOT NULL THEN [1] ELSE [] END | MERGE (lab)<-[:DRUG_HELD_BY]-(drug))
            FOREACH (ignored IN CASE WHEN lab IS NULL THEN [1] ELSE [] END |
               MERGE (fallback:Company:Ansm {name: labName})
               MERGE (fallback)<-[:DRUG_HELD_BY]-(drug)
            )
            RETURN true
        """.trimIndent(), mapOf(Pair("rows", rows), Pair("threshold", labNameSimilarity)))
    }

    private fun createIndices(session: Session) {
        session.beginTransaction().use {
            it.run("CREATE INDEX ON :Drug(name)")
            it.run("CREATE CONSTRAINT ON (d:Drug) ASSERT d.cisCode IS UNIQUE")
            it.run("CREATE INDEX ON :Ansm(name)")
            it.success()
        }
    }

}
