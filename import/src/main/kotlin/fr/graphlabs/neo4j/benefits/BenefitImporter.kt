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
package fr.graphlabs.neo4j.benefits

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

class BenefitImporter(boltUri: String, username: String? = null, password: String? = null) {

    private val database =
            if (username == null) {
                GraphDatabase.driver(boltUri)
            } else {
                GraphDatabase.driver(boltUri, AuthTokens.basic(username, password.orEmpty()))
            }

    fun import(reader: Reader, commitPeriod: Int = 500) {
        reader.use {
            streamRows(it)
                    .asSequence()
                    .batch(commitPeriod)
                    .forEach {
                        val rows = it
                        database.session(AccessMode.WRITE).use {
                            upsertBenefitGraph(it, rows)
                        }
                    }
        }
        database.session(AccessMode.WRITE).use {
            createIndices(it)
        }
    }

    private fun streamRows(reader: Reader): Stream<Row> {
        val columns = Array<String?>(37, { null })
        columns[0] = "lab_identifier"
        columns[4] = "benefit_recipient_type"
        columns[6] = "last_name"
        columns[7] = "first_name"
        columns[8] = "specialty_code"
        columns[9] = "specialty_name"
        columns[32] = "benefit_date"
        columns[33] = "benefit_amount"
        columns[34] = "benefit_type"

        val format = CsvPreference.Builder('"', ';'.toInt(), "\n").build()
        return Streams.fromIterator(CsvMapIterator(columns, reader, skipHeader = true, prefs = format)).map {
            @Suppress("UNCHECKED_CAST")
            val row = it.toMutableMap() as MutableRow
            row.setValuesUpperCase(Locale.FRENCH)
            parseBenefitDate(row)
            row.toMap()
        }.filter {
            val benefitRecipientType = it["benefit_recipient_type"]
            benefitRecipientType !== null && benefitRecipientType.toString() == "[PRS]"
        }
    }

    private fun upsertBenefitGraph(session: Session, rows: List<Row>): StatementResult {
        return session.run("""
            UNWIND {rows} AS row
            MERGE (hp:HealthProfessional {first_name: row.first_name, last_name: row.last_name})
            MERGE (ms:MedicalSpecialty {code: row.specialty_code, name: row.specialty_name})
            MERGE (ms)<-[:SPECIALIZES_IN]-(hp)
            MERGE (y:Year {year: row.year})
            MERGE (y)<-[:MONTH_IN_YEAR]-(m:Month {month: row.month})
            MERGE (m)<-[:DAY_IN_MONTH]-(d:Day {day: row.day})
            MERGE (bt:BenefitType {type: row.benefit_type})
            CREATE (b:Benefit {amount: row.benefit_amount})
            CREATE (b)-[:GIVEN_AT_DATE]->(d)
            CREATE (b)-[:HAS_BENEFIT_TYPE]->(bt)
            MERGE (lab:Company {identifier:row.lab_identifier})
            CREATE (lab)-[:HAS_GIVEN_BENEFIT]->(b)
            CREATE (hp)<-[:HAS_RECEIVED_BENEFIT]-(b)
            RETURN true""".trimIndent(), mapOf(Pair("rows", rows)))
    }

    private fun createIndices(session: Session) {
        session.beginTransaction().use {
            it.run("CREATE INDEX ON :HealthProfessional(first_name, last_name)")
            it.run("CREATE CONSTRAINT ON (ms:MedicalSpecialty) ASSERT ms.code IS UNIQUE")
            it.run("CREATE CONSTRAINT ON (y:Year) ASSERT y.year IS UNIQUE")
            it.run("CREATE CONSTRAINT ON (m:Month) ASSERT m.month IS UNIQUE")
            it.run("CREATE CONSTRAINT ON (d:Day) ASSERT d.day IS UNIQUE")
            it.run("CREATE CONSTRAINT ON (bt:BenefitType) ASSERT bt.type IS UNIQUE")
            it.success()
        }
    }

    private fun parseBenefitDate(row: MutableRow) {
        val dateElements = row["benefit_date"]!!.toString().split("/")
        row["year"] = dateElements[2]
        row["month"] = dateElements[1]
        row["day"] = dateElements[0]
        row.remove("benefit_date")
    }


}
