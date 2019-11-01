import com.spotify.entity.schema.TrackEntity2
import com.spotify.scio._
import com.spotify.scio.parquet.avro._
import org.apache.avro.Schema
import org.apache.parquet.filter2.predicate.FilterPredicate

object ParquetTest {
  val input =
    "gs://scio-playground-eu/parquet/TrackEntity2Parquet"
    // "gs://entity-track2-parquet/TrackEntity2Parquet/2019-10-31/20191101T045251.927009-7ce9bc6cf01a/"

  def main(args: Array[String]): Unit = {
    run("ParquetRead.Baseline")

    run("ParquetRead.Projection1",
      Projection[TrackEntity2](
        _.getTrackGid,
        _.getMasterMetadata,
        _.getEchoNestSong,
        _.getAcousticVector2,
        _.getRecording,
        _.getGlobalPopularity,
        _.getRegionalPopularity))
    run("ParquetRead.Projection2",
      Projection[TrackEntity2](
        _.getTrackGid,
        _.getMasterMetadata,
        _.getEchoNestSong,
        _.getAcousticVector2,
        _.getRecording))
    run("ParquetRead.Projection3",
      Projection[TrackEntity2](
        _.getTrackGid,
        _.getMasterMetadata))
    run("ParquetRead.Projection4",
      Projection[TrackEntity2](_.getTrackGid))

    run("ParquetRead.Filter1",
      predicate = Predicate[TrackEntity2](_.getGlobalPopularity.getRank < 1000000L))
    run("ParquetRead.Filter2",
      predicate = Predicate[TrackEntity2](_.getGlobalPopularity.getRank < 100000L))
    run("ParquetRead.Filter3",
      predicate = Predicate[TrackEntity2](_.getGlobalPopularity.getRank < 10000L))
    run("ParquetRead.Filter4",
      predicate = Predicate[TrackEntity2](_.getGlobalPopularity.getRank < 1000L))
  }

  def run(appName: String, projection: Schema = null, predicate: FilterPredicate = null): Unit = {
    val argz = Array(
      "--runner=DataflowRunner",
      "--numWorkers=10",
      "--maxNumWorkers=10",
      "--workerMachineType=n1-standard-4",
      "--autoscalingAlgorithm=NONE",
      "--project=scio-playground",
      "--region=europe-west1")

    val (sc, _) = ContextAndArgs(argz)
    sc.setAppName(appName)

    sc.parquetAvroFile[TrackEntity2](input, projection, predicate)
      .map(_ => 0)
      .count

    sc.run()
  }
}
