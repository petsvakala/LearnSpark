# The sparklyr package aids in using the “push compute, collect results” principle.

# For more common data manipulation tasks, sparklyr provides a backend for
# dplyr. This means you can use dplyr verbs with which you ’re already familiar in R,
# and then sparklyr and dplyr will translate those actions into Spark SQL statements,
# which are generally more compact and easier to read than SQL statements


# Chapter 5 from the book R for Data Science by 
# Hadley Wickham and Garrett Grolemund(O ’Reilly) is a great resource to learn dplyr in depth.

library(sparklyr)
library(dplyr)

# initiate Spark
options(spark.install.dir = "C:/datasciense/spark/")
sc <- spark_connect(master = "local", version = "2.3")

# Load data directly to Spark not to R
cars <- copy_to(sc, mtcars)
# NB use copy_to only with small tables from R.
# Otherwise use specialized data transfer tools.

summarize_all(cars, mean) %>%
  show_query()


cars %>%
  mutate(transmission = ifelse(am == 0, "automatic", "manual")) %>%
  group_by(transmission) %>%
  summarise_all(mean) %>%
    show_query()

# built in functions
# Spark SQL is based on Hive’s SQL conventions and functions, 
# and it is possible to call all these functions using dplyr as well. 
# This means that we can use any Spark SQL functions to accomplish operations 
# that might not be available via dplyr
summarise(cars, mpg_percentile = percentile(mpg, 0.25)) # Hive function
summarise(cars, mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75))) # Hive array function
# expand list
summarise(cars, mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75))) %>%
  mutate(mpg_percentile = explode(mpg_percentile))
# list of hive functions
# https://therinspark.com/appendix.html#hive-functions

# Spark function for correlation
ml_corr(cars)

# corr package has backend for Spark so when Spark object is used computation takes place in Spark
library(corrr)
correlate(cars, use = "complete.obs", method = "pearson") %>%
  shave() %>%
  rplot()

# using GGPLOT2
library(ggplot2)
#plotting inside R
ggplot(aes(as.factor(cyl), mpg), data = mtcars) + geom_col()

# Be sure to transform and then collect, in that order; 
# if collect() is run first, R will try to ingest the entire dataset from Spark
car_group <- cars %>%
  group_by(cyl) %>%
  summarise(mpg = sum(mpg, na.rm = TRUE)) %>%
  collect() %>%
  print()
# now display data that was preprocessed using Spark
ggplot(aes(as.factor(cyl), mpg), data = car_group) +
  geom_col(fill = "#999999") + coord_flip()

# we recommend that you read R Graphics Cookbook, by Winston Chang (O’Reilly) to learn
# additional visualization techniques applicable to Spark

# The dbplot package provides helper functions for plotting with remote data.
library(dbplot)
# The dbplot_histogram() function makes Spark calculate the bins 
# and the count per bin and outputs a ggplot object, which we 
# can further refine by adding more steps to the plot object. 
cars %>%
dbplot_histogram(mpg, binwidth = 3) +
labs(title = "MPG Distribution",
     subtitle = "Histogram over miles per gallon")

# However, for scatter plots, no amount of “pushing the computation”
#  to Spark will help with this problem because the 
# data must be plotted in individual dots.
ggplot(aes(mpg, wt), data = mtcars) +
  geom_point()

# You can use dbplot_raster() to create a scatter-like plot in Spark,
#  while only retrieving (collecting) a small subset of the remote dataset:
dbplot_raster(cars, mpg, wt, resolution = 16)

# Tip: You can also use dbplot to retrieve the raw data and visualize by other means; 
# to retrieve the aggregates, but not the plots, use db_compute_bins(), 
# db_compute_count(), db_compute_raster(), and db_compute_boxplot().


# modelling
cars %>%
  ml_linear_regression(mpg ~ .) %>%
  summary()

cars %>%
  ml_generalized_linear_regression(mpg ~ hp + cyl) %>%
  summary()

# Caching
cached_cars <- cars %>%
  mutate(cyl = paste0("cyl_", cyl)) %>%
  compute("cached_cars")

cached_cars %>%
  ml_linear_regression(mpg ~ .) %>%
  summary()

# To communicate effectively, we need to use artifacts such as reports and presentations;
# these are common output formats that we can create in R, using R Markdown.
spark_disconnect(sc)

install.packages("installr")
library(installr)

install.pandoc()
pandoc_version()


library(rmarkdown)
library(knitr)
render("report.Rmd")
# file:///C:/datasciense/LearnSpark/report.html

