name := "es-diagnostics-investigator"

scalaVersion := "2.12.4"
val akkaVersion = "2.5.11"
val akkaOrg = "com.typesafe.akka"

resolvers ++= Seq("Akka Snapshots" at "https://repo.akka.io/snapshots/",
              "Akka releases" at  "http://repo.typesafe.com/typesafe/releases") ++ Seq(Opts.resolver.sonatypeReleases)

val akkaDeps = Seq(
  akkaOrg %% "akka-actor"           % akkaVersion,
  akkaOrg %% "akka-contrib"         % akkaVersion,
  akkaOrg %% "akka-agent"           % akkaVersion,
  akkaOrg %% "akka-stream"          % akkaVersion,
  akkaOrg %% "akka-stream-testkit"  % akkaVersion  % "test",
  akkaOrg %% "akka-http"    % "10.0.10",
  akkaOrg %% "akka-testkit"         % akkaVersion  % "test"
)

val commonDeps = Seq(
  "org.json4s"        %% "json4s-jackson"       % "3.5.3",
  "org.json4s"        %% "json4s-native"        % "3.5.3",
  "org.scalatest"     %% "scalatest"            % "3.0.0"             % "test",
  "org.apache.avro"    % "avro"                 % "1.8.0",
  "joda-time"          % "joda-time"            % "2.7",
  "org.slf4j"          % "slf4j-simple"         % "1.7.25"
)

libraryDependencies ++= akkaDeps ++ commonDeps

//fork in run := true
mainClass in(Compile, run) := Some("org.sample.Main")

logBuffered in Test := false

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")