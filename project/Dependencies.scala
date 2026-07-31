import sbt.*

object Dependencies {
  object Versions {
    val scala3 = "3.7.4"
    val scalaSlurm = "0.1.0-SNAPSHOT"
    val catsEffect = "3.7.0"
    val fs2 = "3.13.0"
    val circe = "0.14.16"
    val munit = "1.3.0"
    val munitCatsEffect = "2.2.0"
    val scalaCheck = "1.19.0"
  }

  object Libraries {
    val remoteExecKernel =
      "io.github.bbuchsbaum" %% "remote-exec-kernel" % Versions.scalaSlurm
    val scalaSlurmCore = "io.github.bbuchsbaum" %% "scala-slurm-core" % Versions.scalaSlurm
    val scalaSlurmProtocol =
      "io.github.bbuchsbaum" %% "scala-slurm-protocol" % Versions.scalaSlurm
    val scalaSlurmWorker = "io.github.bbuchsbaum" %% "scala-slurm-worker" % Versions.scalaSlurm
    val scalaSlurmManaged =
      "io.github.bbuchsbaum" %% "scala-slurm-managed" % Versions.scalaSlurm
    val scalaSlurmLocal = "io.github.bbuchsbaum" %% "scala-slurm-local" % Versions.scalaSlurm
    val scalaSlurmSsh = "io.github.bbuchsbaum" %% "scala-slurm-ssh" % Versions.scalaSlurm
    val catsEffect = "org.typelevel" %% "cats-effect" % Versions.catsEffect
    val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit" % Versions.catsEffect
    val fs2Core = "co.fs2" %% "fs2-core" % Versions.fs2
    val fs2Io = "co.fs2" %% "fs2-io" % Versions.fs2
    val circeCore = "io.circe" %% "circe-core" % Versions.circe
    val circeParser = "io.circe" %% "circe-parser" % Versions.circe
    val munit = "org.scalameta" %% "munit" % Versions.munit
    val munitScalaCheck = "org.scalameta" %% "munit-scalacheck" % Versions.munit
    val munitCatsEffect = "org.typelevel" %% "munit-cats-effect" % Versions.munitCatsEffect
    val scalaCheck = "org.scalacheck" %% "scalacheck" % Versions.scalaCheck
  }
}
