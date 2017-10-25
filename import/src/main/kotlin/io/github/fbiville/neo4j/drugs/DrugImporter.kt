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
package io.github.fbiville.neo4j.drugs

import io.github.fbiville.neo4j.Environment
import io.github.fbiville.neo4j.batch
import org.neo4j.driver.v1.AccessMode
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.StatementResult
import java.io.BufferedReader
import java.io.Reader
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
        BufferedReader(reader).use {
            streamRows(it.lines())
                    .asSequence()
                    .batch(commitPeriod)
                    .forEach {
                        val rows = it
                        database.session(AccessMode.WRITE).use {
                            importDrugGraph(it, rows, labNameSimilarity)
                        }
                    }
        }
    }

    private fun streamRows(rows: Stream<String>): Stream<Map<String, Any>> {
        return rows
                .map {
                    val fields = it.split("\t")
                    mapOf(
                            Pair("cisCode", fields[0].trim().toUpperCase(Environment.locale)),
                            Pair("drugName", fields[1].trim().toUpperCase(Environment.locale)),
                            Pair("labNames", fields[10].toUpperCase(Environment.locale).split(";").map { it.trim() })
                    )
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

}