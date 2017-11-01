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

import fr.graphlabs.neo4j.labs.Lab
import org.neo4j.driver.v1.AccessMode
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.Record
import org.springframework.stereotype.Repository

@Repository
class BenefitRepository(private val driver: Driver) {

    fun findAllByHealthProfessional(firstName: String, lastName: String): List<Benefit> {
        driver.session(AccessMode.READ).use {
            val parameters = mapOf(Pair("first_name", firstName), Pair("last_name", lastName))
            val result = it.run("""
                MATCH (hp:HealthProfessional {first_name: {first_name}, last_name: {last_name}}),
                      (hp)<-[:HAS_RECEIVED_BENEFIT]-(b:Benefit),
                      (b)<-[:HAS_GIVEN_BENEFIT]-(lab:Company),
                      (b)-[:HAS_BENEFIT_TYPE]->(bt:BenefitType),
                      (b)-[:GIVEN_AT_DATE]->(d:Day)-[:DAY_IN_MONTH]->(m:Month)-[:MONTH_IN_YEAR]->(y:Year)
                RETURN bt {.type}, b {.amount}, d {.day}, m {.month}, y {.year}, lab {.identifier, .name}
                ORDER BY y.year, m.month, d.day, b.amount DESC
            """.trimIndent(), parameters)

            return result.list().map(this::toBenefit)
        }
    }

    private fun toBenefit(record: Record): Benefit {
        return Benefit(
                BenefitType(record["bt"].asMap()["type"].toString()),
                record["b"].asMap()["amount"].toString(),
                toBenefitDate(record),
                toLab(record)
        )
    }

    private fun toLab(record: Record): Lab {
        val lab = record["lab"].asMap()
        return Lab(
                lab["identifier"].toString(),
                lab["name"].toString()
        )
    }

    private fun toBenefitDate(record: Record): BenefitDate {
        return BenefitDate(
                record["y"].asMap()["year"].toString(),
                record["m"].asMap()["month"].toString(),
                record["d"].asMap()["day"].toString()
        )
    }

}
