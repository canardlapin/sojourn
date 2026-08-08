import sbt.*

object Dependencies {
  object Versions {
    val scala3 = "3.7.4"
    // Immutable pin against a locally (and eventually centrally) published slurm4s release.
    // Source SHA recorded in slurm4s.sha for the optional sibling integration job only.
    val slurm4s = "0.1.0"
    val catsEffect = "3.7.0"
    val fs2 = "3.13.0"
    val circe = "0.14.16"
    val munit = "1.3.0"
    val munitCatsEffect = "2.2.0"
    val scalaCheck = "1.19.0"
  }

  object Libraries {
    val remoteExecKernel =
      "io.github.bbuchsbaum" %% "remote-exec-kernel" % Versions.slurm4s
    val slurm4sCore = "io.github.bbuchsbaum" %% "slurm4s-core" % Versions.slurm4s
    val slurm4sProtocol =
      "io.github.bbuchsbaum" %% "slurm4s-protocol" % Versions.slurm4s
    val slurm4sWorker = "io.github.bbuchsbaum" %% "slurm4s-worker" % Versions.slurm4s
    val slurm4sManaged =
      "io.github.bbuchsbaum" %% "slurm4s-managed" % Versions.slurm4s
    val slurm4sLocal = "io.github.bbuchsbaum" %% "slurm4s-local" % Versions.slurm4s
    val slurm4sSsh = "io.github.bbuchsbaum" %% "slurm4s-ssh" % Versions.slurm4s
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
