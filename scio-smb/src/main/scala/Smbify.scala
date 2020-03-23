import java.util.function.Consumer

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.smb._
import com.spotify.scio.values.SCollection
import org.apache.avro.specific.{SpecificRecordBase => SR}
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, BucketMetadata}
import org.apache.beam.sdk.values.TupleTag

import scala.collection.JavaConverters._
import scala.reflect.{ClassTag => CT}

object SmartSyntax {
  implicit class SmartScioContext(private val sc: ScioContext) extends AnyVal {
    def smart[K: CT : Coder]: Smart[K] = new Smart[K](sc)
  }
}

trait SmartUtil {
  // FIXME: optimize these 2
  def supportsSmb[K](kCt: CT[K], sources: Seq[String]*): Boolean = true
  def keyFn[K, V](kCt: CT[K], paths: Seq[String]): V => K =
    BucketMetadata.from(paths.head).extractKey(_)

  def cls[T](ct: CT[T]): Class[T] = ct.runtimeClass.asInstanceOf[Class[T]]
}

class Smart[K](val sc: ScioContext)(implicit val kCt: CT[K], kCoder: Coder[K])
  extends SmartUtil {
  def fromAvro[V1 <: SR: CT: Coder, W1: Coder](
    paths1: Seq[String]
  )(fn1: V1 => W1): Smart1[K, V1, W1] =
    new Smart1(sc, paths1, fn1)
}

class Smart1[K, V1 <: SR, W1](val sc: ScioContext, val paths1: Seq[String], val fn1: V1 => W1)(
  implicit val kCt: CT[K],
  val kCoder: Coder[K],
  val v1Ct: CT[V1],
  val v1Coder: Coder[V1],
  val w1Coder: Coder[W1]
) extends SmartUtil {
  def and[V2 <: SR: CT: Coder, W2: Coder](paths2: Seq[String])(
    fn2: V2 => W2
  ): Smart2[K, V1, W1, V2, W2] =
    new Smart2(sc, paths1, fn1, paths2, fn2)
}

class Smart2[K, V1 <: SR, W1, V2 <: SR, W2](
  val sc: ScioContext,
  val paths1: Seq[String],
  val fn1: V1 => W1,
  val paths2: Seq[String],
  val fn2: V2 => W2
)(
  implicit val kCt: CT[K],
  val kCoder: Coder[K],
  val v1Ct: CT[V1],
  val v1Coder: Coder[V1],
  val w1Coder: Coder[W1],
  val v2Ct: CT[V2],
  val v2Coder: Coder[V2],
  val w2Coder: Coder[W2]
) extends SmartUtil {
  def and[V3 <: SR: CT: Coder, W3: Coder](paths3: Seq[String])(
    fn3: V3 => W3
  ): Smart3[K, V1, W1, V2, W2, V3, W3] =
    new Smart3(sc, paths1, fn1, paths2, fn2, paths3, fn3)

  def join: SCollection[(K, (W1, W2))] =
    if (supportsSmb(kCt, paths1, paths2)) {
      sc.sortMergeJoin(cls(kCt),
        AvroSortedBucketIO
          .read(new TupleTag[V1]("v1"), cls(v1Ct))
          .from(paths1.asJava),
        AvroSortedBucketIO
          .read(new TupleTag[V2]("v2"), cls(v2Ct))
          .from(paths2.asJava))
        .map { case (k, (v1, v2)) =>
          (k, (fn1(v1), fn2(v2)))
        }
    } else {
      val kf1 = keyFn(kCt, paths1)
      val kf2 = keyFn(kCt, paths2)
      val s1 = sc.unionAll(paths1.map(p => sc.avroFile[V1](p))).map(fn1).keyBy(kf1)
      val s2 = sc.unionAll(paths2.map(p => sc.avroFile[V2](p))).map(fn2).keyBy(kf2)
      s1.join(s2)
    }

  def cogroup: SCollection[(K, (Iterable[W1], Iterable[W2]))] =
    if (supportsSmb(kCt, paths1, paths2)) {
      sc.sortMergeCoGroup(
        cls(kCt),
        AvroSortedBucketIO
          .read(new TupleTag[V1]("v1"), cls(v1Ct))
          .from(paths1.asJava),
        AvroSortedBucketIO
          .read(new TupleTag[V2]("v2"), cls(v2Ct))
          .from(paths2.asJava))
        .map { case (k, (v1, v2)) =>
          (k, (v1.map(fn1), v2.map(fn2)))
        }
    } else {
      val kf1 = keyFn(kCt, paths1)
      val kf2 = keyFn(kCt, paths2)
      val s1 = sc.unionAll(paths1.map(p => sc.avroFile[V1](p))).map(fn1).keyBy(kf1)
      val s2 = sc.unionAll(paths2.map(p => sc.avroFile[V2](p))).map(fn2).keyBy(kf2)
      s1.cogroup(s2)
    }

  def transform[W <: SR : CT : Coder](path: String, keyField: String, numBuckets: Int, numShards: Int = 1)
                  (fn: (K, (Iterable[W1], Iterable[W2]), Consumer[W]) => Unit): ClosedTap[Nothing] =
    if (supportsSmb(kCt, paths1, paths2)) {
      sc.sortMergeTransform(
        cls(kCt),
        AvroSortedBucketIO
          .read(new TupleTag[V1]("v1"), cls(v1Ct))
          .from(paths1.asJava),
        AvroSortedBucketIO
          .read(new TupleTag[V2]("v2"), cls(v2Ct))
          .from(paths2.asJava))
        .to(
          AvroSortedBucketIO
            .write(cls(kCt), keyField, cls(implicitly[CT[W]]))
            .to(path)
            .withNumBuckets(numBuckets)
            .withNumShards(numShards)
        )
        .via { case (k, (v1, v2), outputCollector) =>
          fn(k, (v1.map(fn1), v2.map(fn2)), outputCollector)
        }
    } else {
      val kf1 = keyFn(kCt, paths1)
      val kf2 = keyFn(kCt, paths2)
      val s1 = sc.unionAll(paths1.map(p => sc.avroFile[V1](p))).map(fn1).keyBy(kf1)
      val s2 = sc.unionAll(paths2.map(p => sc.avroFile[V2](p))).map(fn2).keyBy(kf2)
      s1.cogroup(s2).flatMap { case (k, (v1, v2)) =>
        val out = Seq.newBuilder[W]
        val consumer = new Consumer[W]() {
          override def accept(t: W): Unit = out += t
        }
        fn(k, (v1, v2), consumer)
        out.result()
      }
      .saveAsAvroFile(path)
      ???
    }
}

class Smart3[K, V1 <: SR, W1, V2 <: SR, W2, V3 <: SR, W3](
  val sc: ScioContext,
  val paths1: Seq[String],
  val fn1: V1 => W1,
  val paths2: Seq[String],
  val fn2: V2 => W2,
  val paths3: Seq[String],
  val fn3: V3 => W3
)(
  implicit val kCt: CT[K],
  val kCoder: Coder[K],
  val v1Ct: CT[V1],
  val v1Coder: Coder[V1],
  val w1Coder: Coder[W1],
  val v2Ct: CT[V2],
  val v2Coder: Coder[V2],
  val w2Coder: Coder[W2],
  val v3Ct: CT[V3],
  val v3Coder: Coder[V3],
  val w3Coder: Coder[W3]
) extends SmartUtil {
  def and[V4 <: SR: CT: Coder, W4: Coder](paths4: Seq[String])(
    fn4: V4 => W4
  ): Smart4[K, V1, W1, V2, W2, V3, W3, V4, W4] =
    new Smart4(sc, paths1, fn1, paths2, fn2, paths3, fn3, paths4, fn4)
}

class Smart4[K, V1 <: SR, W1, V2 <: SR, W2, V3 <: SR, W3, V4 <: SR, W4](
  val sc: ScioContext,
  val paths1: Seq[String],
  val fn1: V1 => W1,
  val paths2: Seq[String],
  val fn2: V2 => W2,
  val paths3: Seq[String],
  val fn3: V3 => W3,
  val paths4: Seq[String],
  val fn4: V4 => W4
)(
  implicit val kCt: CT[K],
  val kCoder: Coder[K],
  val v1Ct: CT[V1],
  val v1Coder: Coder[V1],
  val w1Coder: Coder[W1],
  val v2Ct: CT[V2],
  val v2Coder: Coder[V2],
  val w2Coder: Coder[W2],
  val v3Ct: CT[V3],
  val v3Coder: Coder[V3],
  val w3Coder: Coder[W3],
  val v4Ct: CT[V4],
  val v4Coder: Coder[V4],
  val w4Coder: Coder[W4]
) extends SmartUtil {}

object Smbify {

  def main(args: Array[String]): Unit = {
    val (sc, argz) = ContextAndArgs(args)
  }
}
