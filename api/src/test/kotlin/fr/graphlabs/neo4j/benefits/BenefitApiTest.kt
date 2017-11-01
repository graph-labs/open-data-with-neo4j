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

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import fr.graphlabs.neo4j.labs.Lab
import org.assertj.core.api.Assertions
import org.junit.Test

class LabsApiTest {

    private val repository = mock<BenefitRepository>()
    private val subject = BenefitApi(repository)

    private val benefits = listOf(
            Benefit(BenefitType("TRANSPORT"),
                    "77000",
                    BenefitDate("2017", "04", "01"),
                    Lab("XYZ", "XYZ labs")),
            Benefit(BenefitType("RESTAURANT"),
                    "23456789",
                    BenefitDate("2019", "12", "01"),
                    Lab("ZZZ", "Sleepy labs"))
    )

    @Test
    fun `returns labs matching the marketed drug package`() {
        whenever(repository.findAllByHealthProfessional("John", "Doe")).thenReturn(benefits)

        val result = subject.findBenefitsByHealthProfessional("John", "Doe")

        Assertions.assertThat(result).isEqualTo(benefits)
    }
}
