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
package io.github.fbiville.neo4j.packages

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

class PackageImporter(boltUri: String, username: String? = null, password: String? = null) {

    private val database =
            if (username == null) {
                GraphDatabase.driver(boltUri)
            } else {
                GraphDatabase.driver(boltUri, AuthTokens.basic(username, password.orEmpty()))
            }

    fun import(reader: Reader, commitPeriod: Int = 500) {
        BufferedReader(reader).use {
            streamRows(it.lines())
                    .asSequence()
                    .batch(commitPeriod)
                    .forEach {
                        val rows = it
                        database.session(AccessMode.WRITE).use {
                            importPackageGraph(it, rows)
                        }
                    }
        }
        database.session(AccessMode.WRITE).use {
            createIndices(it)
        }
    }

    private fun streamRows(rows: Stream<String>): Stream<Map<String, Any>> {
        return rows
                .map {
                    val fields = it.split("\t")
                    mapOf(
                            Pair("cisCode", fields[0].trim().toUpperCase(Environment.locale)),
                            Pair("packageName", fields[2].trim().toUpperCase(Environment.locale)),
                            Pair("cip13Code", fields[6].trim().toUpperCase(Environment.locale))
                    )
                }
    }

    private fun importPackageGraph(session: Session, rows: List<Map<String, Any>>): StatementResult {
        return session.run("""
            |UNWIND {rows} AS row
            |MERGE (drug:Drug {cisCode: row.cisCode})
            |ON CREATE SET drug:DrugFromPackage
            |MERGE (package :Package {name: row.packageName, cip13Code: row.cip13Code})
            |MERGE (package)<-[:DRUG_PACKAGED_AS]-(drug)
            |RETURN true""".trimMargin(), mapOf(Pair("rows", rows)))
    }

    private fun createIndices(session: Session) {
        session.beginTransaction().use {
            it.run("CREATE INDEX ON :Package(name)")
            it.run("CREATE CONSTRAINT ON (p:Package) ASSERT p.cip13Code IS UNIQUE")
            it.run("CREATE CONSTRAINT ON (d:DrugFromPackage) ASSERT d.cisCode IS UNIQUE")
            it.success()
        }
    }
}