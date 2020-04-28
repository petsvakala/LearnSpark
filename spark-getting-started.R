# using book: https://therinspark.com/starting.html

packageVersion("sparklyr")
library(sparklyr)

spark_available_versions()
spark_installed_versions()

options(spark.install.dir = "C:/datasciense/spark/")
spark_install("2.3")

# spark_uninstall()


sc <- spark_connect(master = "local", version = "2.3")

# copy cmtars dataset to Spark
cars <- copy_to(sc, mtcars)
str(cars)
cars

# access Spark web console
spark_web(sc)

# use SQL to analyse spark data
library(DBI)
dbGetQuery(sc, "SELECT count(*) FROM mtcars")

# or Dplyr
library(dplyr)
count(cars)

# visualise
select(cars, hp, mpg) %>%
  sample_n(100) %>%
  collect() %>%
  plot()

# model
model <- ml_linear_regression(cars, mpg ~ hp)
model

# predict
model %>%
  ml_predict(copy_to(sc, data.frame(hp = 250 + 10 * 1:10))) %>%
  transmute(hp = hp, mpg = prediction) %>%
  full_join(select(cars, hp, mpg)) %>%
  collect() %>%
  plot()

#export data
spark_write_csv(cars, "cars.csv")

# for working with JSON
install.packages("sparklyr.nested")

sparklyr.nested::sdf_nest(cars, hp) %>%
  group_by(cyl) %>%
  summarise(data = collect_list(data))

# you can but shouldn't always apply spark_apply function
cars %>% spark_apply(~round(.x))

# working with streaming data
dir.create("input") # C:\datasciense\LearnSpark\input
write.csv(mtcars, "input/cars_1.csv", row.names = F)

stream <- stream_read_csv(sc, "input/") %>%
    select(mpg, cyl, disp) %>%
    stream_write_csv("output/")

dir("output", pattern = ".csv")

# Write more data into the stream source
write.csv(mtcars, "input/cars_2.csv", row.names = F)
# Check the contents of the stream destination
dir("output", pattern = ".csv")
stream_stop(stream) # stop stream


spark_log(sc) # extract spark log
spark_log(sc, filter = "sparklyr")

# disconnect Spark
spark_disconnect(sc)

#disconnect all
spark_disconnect_all()