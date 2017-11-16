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
package fr.graphlabs.neo4j.packages

import fr.graphlabs.neo4j.*
import fr.graphlabs.neo4j.agnostic.collections.Streams
import fr.graphlabs.neo4j.agnostic.collections.batch
import fr.graphlabs.neo4j.agnostic.collections.setValuesUpperCase
import fr.graphlabs.neo4j.agnostic.csv.CsvMapIterator
import org.neo4j.driver.v1.*
import org.supercsv.prefs.CsvPreference
import java.io.Reader
import java.util.*
import java.util.stream.Stream
import kotlin.streams.asSequence

class PackageImporter(boltUri: String, username: String? = null, password: String? = null) {

    private val database =
            if (username == null) {
                GraphDatabase.driver(boltUri)
            } else {
                GraphDatabase.driver(boltUri, AuthTokens.basic(username, password.orEmpty()))
            }

    fun import(reader: Reader, commitPeriod: Int = 500) {
        database.session(AccessMode.WRITE).use {
            createIndices(it)
        }
        reader.use {
            streamRows(it)
                    .asSequence()
                    .batch(commitPeriod)
                    .forEach {
                        val rows = it
                        database.session(AccessMode.WRITE).use {
                            importPackageGraph(it, rows)
                        }
                    }
        }
    }

    private fun streamRows(reader: Reader): Stream<Row> {
        val columns = Array<String?>(13, { null })
        columns[0] = "cis_code"
        columns[2] = "package_name"
        columns[6] = "cip13_code"
        val format = CsvPreference.Builder('£', '\t'.toInt(), "\n").build()
        return Streams.fromIterator(CsvMapIterator(columns, reader, skipHeader = false, prefs = format))
                .map {
                    @Suppress("UNCHECKED_CAST")
                    val rows = it.toMutableMap() as MutableRow
                    rows.setValuesUpperCase(Locale.FRENCH)
                    rows
                }
    }

    private fun importPackageGraph(session: Session, rows: List<Row>): StatementResult {
        return session.run("""
             UNWIND {rows} AS row
             MERGE (drug:Drug {cis_code: row.cis_code})
             ON CREATE SET drug:DrugFromPackage
             MERGE (package :Package {name: row.package_name}) ON CREATE SET package.cip13_code = row.cip13_code
             MERGE (package)<-[:DRUG_PACKAGED_AS]-(drug)
             RETURN true""".trimIndent(), mapOf(Pair("rows", rows)))
    }

    private fun createIndices(session: Session) {
        session.beginTransaction().use {
            it.run("CREATE INDEX ON :Package(name)")
            it.run("CREATE CONSTRAINT ON (p:Package) ASSERT p.cip13_code IS UNIQUE")
            it.run("CREATE CONSTRAINT ON (d:DrugFromPackage) ASSERT d.cis_code IS UNIQUE")
            it.success()
        }
    }
}
