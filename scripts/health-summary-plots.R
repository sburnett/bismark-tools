library(ggplot2)
library(plyr)

args <- commandArgs(trailingOnly = TRUE)
dirname <- args[1]
output.dir <- paste(dirname, "plots", sep="/")

format_bytes <- function(...) {
  function(x) {
    limits <- c(1e0,   1e3,
                1e6,   1e9,   1e12,  1e15,  1e18,
                1e21,  1e24)
    prefix <- c("B",   "KB",
                "MB",   "GB",   "TB",   "PB",   "EB",
                "ZB",   "YB")
  
    # Vector with array indices according to position in intervals
    i <- findInterval(abs(x), limits)
  
    # Set prefix to " " for very small values < 1e-24
    i <- ifelse(i==0, which(limits == 1e0), i)

    paste(format(round(x/limits[i], 0),
                 trim=TRUE, scientific=FALSE, ...),
          prefix[i])
  }
}

plot.daily.memory.summary <- function(memory.usage, prefix="") {
    p <- ggplot(memory.usage) +
        stat_summary(aes(x=timestamp, y=usage*1024), geom="smooth", fun.y="median", fun.ymin=function(y) quantile(y, 0.05), fun.ymax=function(y) quantile(y, 0.95)) +
        labs(x="Date", y="Memory usage", title="5th, 50th and 95th percentiles of memory usage on all routers") +
        scale_y_continuous(labels=format_bytes()) +
        expand_limits(y=0)
    filename <- paste(output.dir, "/", prefix, "memory-usage.png", sep="")
    ggsave(filename, p, dpi=120, width=8, height=3)
}

plot.daily.filesystem.summary <- function(filesystem.usage, prefix="") {
    if (length(filesystem.usage$filesystem) < 10) {
        return()
    }
    p <- ggplot(filesystem.usage) +
        stat_summary(aes(x=timestamp, y=usage*1024), geom="smooth", fun.y="median", fun.ymin=function(y) quantile(y, 0.05), fun.ymax=function(y) quantile(y, 0.95)) +
        labs(x="Date", y="Filesystem usage", title=paste("5th, 50th and 95th percentiles of", filesystem.usage$filesystem, "filesystem usage on all routers")) +
        scale_y_continuous(labels=format_bytes()) +
        expand_limits(y=0)
    filesystem <- gsub("/", "", filesystem.usage$filesystem, fixed=TRUE)
    filename <- paste(output.dir, "/", prefix, "filesystem-usage-", filesystem, ".png", sep="")
    ggsave(filename, p, dpi=120, width=8, height=3)
}

memory.usage <- read.csv(paste(dirname, "memory-usage-summary.csv", sep="/"))
memory.usage$timestamp <- as.POSIXct(memory.usage$timestamp, origin="1970-01-01")
memory.usage <- memory.usage[memory.usage$timestamp > as.POSIXct("2013-03-01"),]
monthly.memory.usage <- memory.usage[as.Date(memory.usage$timestamp) >= Sys.Date() - 30,]

filesystem.usage <- read.csv(paste(dirname, "filesystem-usage-summary.csv", sep="/"))
filesystem.usage$timestamp <- as.POSIXct(filesystem.usage$timestamp, origin="1970-01-01")
filesystem.usage <- filesystem.usage[filesystem.usage$timestamp > as.POSIXct("2013-03-01"),]
monthly.filesystem.usage <- filesystem.usage[as.Date(filesystem.usage$timestamp) >= Sys.Date() - 30,]

plot.daily.memory.summary(memory.usage)
plot.daily.memory.summary(monthly.memory.usage, "monthly-")
d_ply(filesystem.usage, .(filesystem), plot.daily.filesystem.summary)
d_ply(monthly.filesystem.usage, .(filesystem), function(d) plot.daily.filesystem.summary(d, "monthly-"))
