lazy val root = (project in file("."))
  .settings(
    name := "aiportAnalysis"
  )

lazy val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

SettingKey[Option[String]]("ide-package-prefix") := Option("com.mainDir")