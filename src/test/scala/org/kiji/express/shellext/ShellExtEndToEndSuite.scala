/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.shellext

import java.io.File
import java.io.FileWriter

import com.google.common.io.Files

import org.kiji.express.KijiSlice
import org.kiji.express.modeling.ExtractEnvironment
import org.kiji.express.modeling.Extractor
import org.kiji.express.modeling.FieldBindingSpec
import org.kiji.express.modeling.ModelDefinition
import org.kiji.express.modeling.ModelEnvironment
import org.kiji.express.modeling.ScoreEnvironment
import org.kiji.express.modeling.Scorer
import org.kiji.express.util.Resources.doAndClose
import org.kiji.express.util.Resources.doAndRelease
import org.kiji.schema.Kiji
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiURI
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.layout.KijiTableLayout
import org.kiji.schema.layout.KijiTableLayouts
import org.kiji.schema.util.InstanceBuilder

/**
 * Provides end-to-end tests for KijiExpress extensions to the KijiSchema DDL Shell language.
 */
class ShellExtEndToEnd extends ShellExtSuite {
  test("The schema shell can be used to launch a batch extract and score.") {
    val tmpDir: File = Files.createTempDir()
    val modelDefFile: File = new File(tmpDir, "model-def.json")
    val modelEnvFile: File = new File(tmpDir, "model-env.json")

    try {
      // Setup the test environment.
      val testLayout: KijiTableLayout = layout(KijiTableLayouts.SIMPLE_TWO_COLUMNS)
      val kiji: Kiji = new InstanceBuilder("default")
          .withTable(testLayout.getName(), testLayout)
              .withRow("row1")
                  .withFamily("family")
                      .withQualifier("column1").withValue("foo")
              .withRow("row2")
                  .withFamily("family")
                      .withQualifier("column1").withValue("bar")
          .build()

      doAndRelease(kiji.openTable(testLayout.getName())) { table: KijiTable =>
        val uri: KijiURI = table.getURI()

        // Create a model definition and environment.
        val request: KijiDataRequest = KijiDataRequest.create("family", "column1")
        val modelDefinition: ModelDefinition = ModelDefinition(
            name = "test-model-definition",
            version = "1.0",
            extractor = classOf[ShellExtEndToEndSuite.DoublingExtractor],
            scorer = classOf[ShellExtEndToEndSuite.UpperCaseScorer])
        val modelEnvironment: ModelEnvironment = ModelEnvironment(
            name = "test-model-environment",
            version = "1.0",
            modelTableUri = uri.toString,
            extractEnvironment = ExtractEnvironment(
                dataRequest = request,
                fieldBindings = Seq(FieldBindingSpec("field", "family:column1")),
                kvstores = Seq()),
            scoreEnvironment = new ScoreEnvironment(
                outputColumn = "family:column2",
                kvstores = Seq()))

        // Write the created model definition and environment to disk.
        doAndClose(new FileWriter(modelDefFile)) { writer =>
          writer.write(modelDefinition.toJson())
        }
        doAndClose(new FileWriter(modelEnvFile)) { writer =>
          writer.write(modelEnvironment.toJson())
        }

        // Run a batch extract + score using the schema-shell.
        val parser = getLoadedParser()
        val parseResult = parser.parseAll(parser.statement,
          """
            |EXTRACT SCORE
            |  USING MODEL DEFINITION INFILE '%s'
            |  USING MODEL ENVIRONMENT INFILE '%s';
          """.stripMargin.format(modelDefFile.getAbsolutePath, modelEnvFile.getAbsolutePath))
        parseResult.get.exec()

        // Validate the results of running the model.
        doAndClose(table.openTableReader()) { reader: KijiTableReader =>
          val v1 = reader
              .get(table.getEntityId("row1"), KijiDataRequest.create("family", "column2"))
              .getMostRecentValue("family", "column2")
              .toString
          val v2 = reader
              .get(table.getEntityId("row2"), KijiDataRequest.create("family", "column2"))
              .getMostRecentValue("family", "column2")
              .toString

          assert("FOOFOO" === v1)
          assert("BARBAR" === v2)
        }
      }
    } finally {
      // Cleanup.
      tmpDir.delete()
      modelDefFile.delete()
      modelEnvFile.delete()
    }
  }
}

object ShellExtEndToEndSuite {
  class DoublingExtractor extends Extractor {
    override val extractFn = extract('field -> 'feature) { field: KijiSlice[String] =>
      val str: String = field.getFirstValue
      str + str
    }
  }

  class UpperCaseScorer extends Scorer {
    override val scoreFn = score('feature) { feature: String =>
      feature.toUpperCase
    }
  }
}