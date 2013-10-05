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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.express.modeling.lib

import org.kiji.annotations.{Inheritance, ApiStability, ApiAudience}
import org.kiji.express.modeling.Trainer
import org.kiji.express._
import org.kiji.express.flow._
import com.twitter.scalding._
import com.twitter.scalding.Tsv

@ApiAudience.Public
@ApiStability.Experimental
@Inheritance.Sealed
final case class KMeansTrainer() extends Trainer {

  class KMeansTrainerJob(numIterations: Int) extends TrainerJob {
    val songsTableURI: String = "kiji://.env/kiji_express_music/songs"

    class WeightedVector(val vector: Map[String, Double], val weight: Double)

    /**
     * Turns a KijiSlice of the top next songs played into a weighted sparse vector.
     */
    def toWeightedSongVector(topNextSongs: KijiSlice[AvroRecord]): WeightedVector = {
      new WeightedVector(
        topNextSongs.getFirstValue()("topSongs").asList().map {
          record => record("song_id").asString() -> record("count").asLong().toDouble
        }.toMap,
        1.0
      )
    }

    /**
     * Gets the distance between two sparse vectors.
     */
    def getDistance(v1: Map[String, Double], v2: Map[String, Double]): Double = {
      math.pow(
        (v1.keySet ++ v2.keySet).map {
        key => math.pow(v1.getOrElse(key, 0.0) - v2.getOrElse(key, 0.0), 2)
      }.sum,
      0.5)
    }

    /**
     * Finds the cluster mean closest to a given sparse vector.
     */
    def findClosestCluster(clusters: List[Map[String, Double]], vector: Map[String, Double]): Int = {
      clusters
        .map {
        getDistance(_, vector)
      }
        .zipWithIndex
        .min
        ._2
    }

    /**
     * Computes the weighted average (mean) of two vectors.
     */
    def weightedAverage(v1: WeightedVector, v2: WeightedVector): WeightedVector = {
      new WeightedVector((v1.vector.keySet ++ v2.vector.keySet).map {
        key =>
          val w1 = v1.weight
          val w2 = v2.weight
          key -> (v1.vector.getOrElse(key, 0.0) * w1 + v2.vector.getOrElse(key, 0.0) * w2) /(w1 + w2)
      }.toMap,
      v1.weight + v2.weight
      )
    }

    /**
     * Runs one EM step of K-Means.
     */
    def runIteration(number: Int, clusterMeans: RichPipe): RichPipe = {
      // Read the top next songs for each song.
      // Turn each song into its sparse vector representation.
      // Write it out
      val assignedSongs = KijiInput(songsTableURI)("info:top_next_songs" -> 'topNextSongs)
        .map('topNextSongs -> 'songVector) {
        toWeightedSongVector
      }
        .project('entityId, 'songVector)
        .crossWithTiny(clusterMeans)
        .map(('songVector, 'clusterMeans) -> 'closestCluster) {
        vm: (WeightedVector, List[Map[String, Double]]) => findClosestCluster(vm._2, vm._1.vector)
      }
        .write(Tsv("express-tutorial/clustered%02d".format(number), ('entityId, 'closestCluster)))

      assignedSongs
        .groupBy('closestCluster) {
        _.reduce('songVector -> 'meanVector) {
          weightedAverage
        }
      }
        .map('meanVector -> 'numSongsInCluster) {
        v: WeightedVector => v.weight.toInt
      }
        .map('meanVector -> 'clusterMean) {
        v: WeightedVector => v.vector
      }
        .discard('meanVector)
        .write(Tsv("express-tutorial/means%02d".format(number)))
        .groupAll {
        _.toList[Map[String, Double]]('clusterMean -> 'clusterMeans)
      }
        .project('clusterMeans)
    }

    // Choose three arbitrary initial cluster locations (so K = 3 here).
    val initialClusters = List[Map[String, Double]](
      Map("song-30" -> 20.0, "song-31" -> 17.0),
      Map("song-6" -> 9.0, "song-9" -> 11.0),
      Map("song-35" -> 7.0, "song-22" -> 18.0))

    // Run four iterations of K-Means.
    var previousMeans: RichPipe = IterableSource(List(initialClusters), 'clusterMeans)
    (1 to numIterations).foreach {
      n =>
        previousMeans = runIteration(n, previousMeans)
    }
  }

  /**
   * Used to define the computation required for the Train phase of the model lifecycle.
   *
   * @param input data sources used during the train phase.
   * @param output data sources used during the train phase.
   * @return true if job succeeds, false otherwise.
   */
  override def train(input: Map[String, Source], output: Map[String, Source]): Boolean = {
    new KMeansTrainerJob(4).run
  }
}