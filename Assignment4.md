Assignment 04 - HPC and SQL
================
Brandyn Ruiz
10/24/2020

The learning objecives are to conduct data scraping and perfrom text
mining.

``` r
library(parallel)
library(RSQLite)
library(DBI)
```

# HPC

## Problem 1: Make sure your code is nice

Rewrite the following R function to make them faster. It is OK (and
recommended) to take a look at Stackoverflow and Google.

``` r
# Total row sums
fun1 <- function(mat) {
  n <- nrow(mat)
  ans <- double(n) 
  for (i in 1:n) {
    ans[i] <- sum(mat[i, ])
  }
  ans
}

fun1alt <- function(mat) {
  # YOUR CODE HERE
  ans <- NULL
  for (i in 1:nrow(mat)){
    ans[i] <- sum(mat[i, ])
  }
  ans
}

# Cumulative sum by row
fun2 <- function(mat) {
  n <- nrow(mat)
  k <- ncol(mat)
  ans <- mat
  for (i in 1:n) {
    for (j in 2:k) {
      ans[i,j] <- mat[i, j] + ans[i, j - 1]
    }
  }
  ans
}

fun2alt <- function(mat) {
  # YOUR CODE HERE
  ans <- mat
  for(i in 1:nrow(mat)){
    for(j in 2:ncol(mat)){
      ans[i, j] <- ans[i, j] + ans[i, j - 1]
    }
  }
  ans
}

# Use the data with this code
set.seed(2315)
dat <- matrix(rnorm(200 * 100), nrow = 200)

# Test for the first
microbenchmark::microbenchmark(
  fun1(dat),
  fun1alt(dat), unit = "relative", check = "equivalent"
)
```

    ## Unit: relative
    ##          expr      min      lq     mean  median       uq      max neval cld
    ##     fun1(dat) 1.000000 1.00000 1.000000 1.00000 1.000000 1.000000   100  a 
    ##  fun1alt(dat) 1.136856 1.11321 1.135422 1.12343 1.140552 1.260677   100   b

``` r
# Test for the second
microbenchmark::microbenchmark(
  fun2(dat),
  fun2alt(dat), unit = "relative", check = "equivalent"
)
```

    ## Unit: relative
    ##          expr      min       lq    mean   median      uq       max neval cld
    ##     fun2(dat) 1.000000 1.000000 1.00000 1.000000 1.00000 1.0000000   100   a
    ##  fun2alt(dat) 1.036307 1.063294 1.01675 1.095542 1.02296 0.8155511   100   a

The last arguement, check = “equivalent”, is included to make suree that
the functions return the same result.

## Problem 2: Make things run faster with parallel computing

The following function allows simulating PI

``` r
sim_pi <- function(n = 1000, i = NULL) {
  p <- matrix(runif(n*2), ncol = 2)
  mean(rowSums(p^2) < 1) * 4
}

# Here is an example of the run
set.seed(156)
sim_pi(1000) # 3.132
```

    ## [1] 3.132

In order to get accurate estimates, we can run this function multiple
times, with the following code:

``` r
# This runs the simulation a 4,000 times, each with 10,000 points
set.seed(1231)
system.time({
  ans <- unlist(lapply(1:4000, sim_pi, n = 10000))
  print(mean(ans))
})
```

    ## [1] 3.14124

    ##    user  system elapsed 
    ##    3.08    0.06    3.17

Rewrite the previous code using `parLapply()` to make it run faster.
Make sure you set the seed using `clusterSetRNGStream()`:

``` r
# YOUR CODE HERE
cl <- makePSOCKcluster(4)
x <- 4000
clusterSetRNGStream(cl, 1231) # Equivalent to `set.seed(1231)`

system.time({
  # YOUR CODE HERE
  ans <- NULL
  print(mean(ans))
  # YOUR CODE HERE
  parallel::stopCluster(cl)
})
```

    ## Warning in mean.default(ans): argument is not numeric or logical: returning NA

    ## [1] NA

    ##    user  system elapsed 
    ##       0       0       0

# SQL

Setup a temporary datavase by running the following chunk

``` r
# Initialize a temporary in memory database
con <- dbConnect(SQLite(), ":memory:")

# Download tables
film <- read.csv(
  "https://raw.githubusercontent.com/ivanceras/sakila/master/csv-sakila-db/film.csv")

film_category <- read.csv(
  "https://raw.githubusercontent.com/ivanceras/sakila/master/csv-sakila-db/film_category.csv")

category <- read.csv(
  "https://raw.githubusercontent.com/ivanceras/sakila/master/csv-sakila-db/category.csv")

# Copy data.frames to database
dbWriteTable(con, "film", film)
dbWriteTable(con, "film_category", film_category)
dbWriteTable(con, "category", category)
```

When you write a new chunk, remember to replace the `r` with `sql,
connection = con`. Some of these questions will require you to use an
inner join. Read more about them here
<https://www.w3schools.com/sql/sql_join_inner.asp>

## Question 1

How many movies is there avaliable in each `rating` category?

``` sql
SELECT rating, COUNT(*) as count
FROM film
GROUP BY rating
ORDER BY count DESC
```

<div class="knitsql-table">

| rating | count |
| :----- | ----: |
| PG-13  |   223 |
| NC-17  |   210 |
| R      |   195 |
| PG     |   194 |
| G      |   180 |

5 records

</div>

From our output above we see that there are more movies with the rating
of PG-13 with a count of 223, following is NC-17 with 210 total, rated R
movies have a value of 195, PG movies with 194, and rated G 180 movies.

## Qustion 2

What is the average replacement cost and rental rate for each `rating`
category?

``` sql
SELECT rating, avg(replacement_cost) as AVGreplacement_cost, avg(rental_rate) as AVGrental_rate
FROM film
GROUP BY rating
ORDER BY AVGreplacement_cost DESC, AVGrental_rate DESC
```

<div class="knitsql-table">

| rating | AVGreplacement\_cost | AVGrental\_rate |
| :----- | -------------------: | --------------: |
| PG-13  |             20.40256 |        3.034843 |
| R      |             20.23103 |        2.938718 |
| NC-17  |             20.13762 |        2.970952 |
| G      |             20.12333 |        2.912222 |
| PG     |             18.95907 |        3.051856 |

5 records

</div>

The greatest movie rating with the highest average replacement cost and
renatal rate are PG-13 movies. The least average replacement cost is
$18.96 with the lowest average rental rate of $3.05 for PG movies.

## Question 3

Incorporate table `category` into the answer to the previous question to
find the name of the most popular category

``` sql
SELECT film.rating, avg(film.replacement_cost) as AVGreplacement_cost,
  avg(film.rental_rate) as AVGrental_rate, category.name
FROM film
JOIN film_category ON film.film_id = film_category.film_id
JOIN category ON category.category_id = film_category.category_id
GROUP BY rating
ORDER BY AVGreplacement_cost DESC, AVGrental_rate DESC
```

<div class="knitsql-table">

| rating | AVGreplacement\_cost | AVGrental\_rate | name        |
| :----- | -------------------: | --------------: | :---------- |
| PG-13  |             20.40256 |        3.034843 | Comedy      |
| R      |             20.23103 |        2.938718 | Horror      |
| NC-17  |             20.13762 |        2.970952 | Documentary |
| G      |             20.12483 |        2.888876 | Horror      |
| PG     |             18.95907 |        3.051856 | Documentary |

5 records

</div>

With incorporating the category table its contents have `category_id`
and `name`, but in order to connect the `category_id` to the *film*
table our *film\_category* table must also be involved. From
*film\_category* we can connect the two datasets with `film_id` and
`category_id`. When arranging the averages of the replacement cost and
average rental rate in descending order, movie rating with PG-13 being
Comedy is the greatest, while PG Documentary movies are the least.
