library(plyr)
library(ggplot2)

args <- commandArgs(trailingOnly = TRUE)
dirname <- args[1]
output.dir <- paste(dirname, "plots", sep="/")

cbbPalette <- c("#000000", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

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

format_duration <- function(...) {
  function(x) {
    limits <- c(1,   60, 3600, 86400)
    prefix <- c("Sec",   "Min", "Hours", "Days")
  
    # Vector with array indices according to position in intervals
    i <- findInterval(abs(x), limits)
  
    # Set prefix to " " for very small values < 1e-24
    i <- ifelse(i==0, which(limits == 1e0), i)

    paste(format(round(x/limits[i], 0),
                 trim=TRUE, scientific=FALSE, ...),
          prefix[i])
  }
}

weight.quantiles <- function(d) {
    data.frame(cbind(X0th=weighted.mean(d$X0th, d$count),
                     X25th=weighted.mean(d$X25th, d$count),
                     X50th=weighted.mean(d$X50th, d$count),
                     X75th=weighted.mean(d$X75th, d$count),
                     X100th=weighted.mean(d$X100th, d$count),
                     nodes=length(d$node)))
}

plot.counts <- function(data) {
    p <- ggplot(data) +
        geom_bar(aes(x=node, y=count)) +
        theme(axis.text.y=element_text(family="mono")) +
        ggtitle(paste("Files uploaded per router for experiment", data$experiment)) +
        labs(y="Files uploaded", x="Router ID") +
        coord_flip()
    filename <- paste(output.dir, "/counts-", data$experiment, ".png", sep="")

    ggsave(filename, p, dpi=120, width=6, height=0.10 * length(unique(data)$node) + 1)
}

plot.all.counts <- function(data) {
    agg.data <- ddply(data, .(experiment), function(e) data.frame(count=sum(e$count), nodes=length(e$node)))
    p <- ggplot(agg.data) +
        geom_bar(aes(x=experiment, y=count, fill=nodes)) +
        ggtitle(paste("Files uploaded per experiment")) +
        labs(y="Files uploaded", x="Experiment") +
        coord_flip()
    filename <- paste(output.dir, "/counts.png", sep="")

    ggsave(filename, p, dpi=120, width=8, height=0.10 * length(levels(data$experiment)) + 1.2)
}

plot.sizes <- function(data) {
    p <- ggplot(data, aes(x=node)) +
        geom_boxplot(aes(ymin=X0th, lower=X25th, middle=X50th, upper=X75th, ymax=X100th, fill=count), stat="identity") +
        theme(axis.text.y=element_text(family="mono")) +
        ggtitle(paste("File sizes per router for experiment", data$experiment)) +
        labs(y="File size", x="Router ID") +
        scale_y_continuous(labels=format_bytes()) +
        coord_flip()
    filename <- paste(output.dir, "/sizes-", data$experiment, ".png", sep="")

    ggsave(filename, p, dpi=120, width=6, height=0.10 * length(unique(data)$node) + 1)
}

plot.sizes.all.experiments <- function(data) {
    agg.data <- ddply(data, .(experiment), weight.quantiles)
    p <- ggplot(agg.data, aes(x=experiment)) +
        geom_boxplot(aes(ymin=`X0th`, lower=`X25th`, middle=`X50th`, upper=`X75th`, ymax=`X100th`), stat="identity") +
        ggtitle("File sizes per experiment") +
        labs(y="File size", x="Experiment") +
        scale_y_continuous(labels=format_bytes()) +
        coord_flip()

    filename <- paste(output.dir, "/sizes.png", sep="")

    ggsave(filename, p, dpi=120, width=8, height=0.15 * length(levels(data$experiment)) + 1)

    return(agg.data)
}

plot.interarrivals <- function(data) {
    p <- ggplot(data) +
        geom_point(aes(x=node, y=X50th, color=count)) +
        theme(axis.text.y=element_text(family="mono")) +
        ggtitle(paste("Median upload interarrival time per router for experiment", data$experiment)) +
        labs(y="Upload interarrival time", x="Router ID") +
        scale_fill_continuous(guide="legend", trans="log") +
        scale_y_continuous(labels=format_duration()) +
        coord_flip()
    filename <- paste(output.dir, "/interarrival-", data$experiment, ".png", sep="")

    ggsave(filename, p, dpi=120, width=6, height=0.10 * length(unique(data$node)) + 1)
}

plot.interarrivals.all.experiments <- function(data) {
    agg.data <- ddply(data, .(experiment), weight.quantiles)
    p <- ggplot(agg.data, aes(x=experiment)) +
        geom_bar(aes(y=X50th, fill=nodes)) +
        ggtitle("Upload interarrival times per experiment") +
        labs(y="Median time between uploads", x="Experiment") +
        scale_y_continuous(labels=format_duration()) +
        coord_flip()

    filename <- paste(output.dir, "/interarrivals.png", sep="")

    ggsave(filename, p, dpi=120, width=8, height=0.15 * length(levels(data$experiment)) + 1)

    return(agg.data)
}

plot.usage <- function(data) {
    p <- ggplot(data, aes(x=node)) +
        geom_boxplot(aes(ymin=X0th*86400/X50th.interarrival, lower=X25th*86400/X50th.interarrival, middle=X50th*86400/X50th.interarrival, upper=X75th*86400/X50th.interarrival, ymax=X100th*86400/X50th.interarrival, fill=count), stat="identity") +
        theme(axis.text.y=element_text(family="mono")) +
        ggtitle(paste("Projected data uploaded per day per router for experiment", data$experiment)) +
        labs(y="Data uploaded per day", x="Router ID") +
        scale_y_continuous(labels=format_bytes()) +
        coord_flip()
    filename <- paste(output.dir, "/usage-", data$experiment, ".png", sep="")

    ggsave(filename, p, dpi=120, width=6, height=0.10 * length(unique(data$node)) + 1)
}

plot.usage.all.experiments <- function(size.summary, agg.interarrivals) {
    agg.data <- ddply(size.summary, .(experiment), weight.quantiles)
    merged <- merge(agg.data, agg.interarrivals, by="experiment", suffixes=c("", ".interarrival"))
    p <- ggplot(merged, aes(x=experiment)) +
        geom_boxplot(aes(ymin=X0th*86400/X50th.interarrival, lower=X25th*86400/X50th.interarrival, middle=X50th*86400/X50th.interarrival, upper=X75th*86400/X50th.interarrival, ymax=X100th*86400/X50th.interarrival), stat="identity") +
        ggtitle("Projected data uploaded from each router per day") +
        labs(y="Data uploaded per day", x="Experiment") +
        scale_y_continuous(labels=format_bytes()) +
        coord_flip()

    filename <- paste(output.dir, "usage.png", sep="/")

    ggsave(filename, p, dpi=120, width=8, height=0.15 * length(levels(merged$experiment)) + 1)
}

plot.sizes.per.day.all.experiments <- function(sizes.per.day) {
    p <- ggplot(sizes.per.day) +
        geom_boxplot(aes(x=experiment, y=count)) +
        ggtitle("Data uploaded from each router per day") +
        labs(y="Data uploaded per day", x="Experiment") +
        scale_y_continuous(labels=format_bytes()) +
        coord_flip()

    filename <- paste(output.dir, "dailyusage.png", sep="/")
    ggsave(filename, p, dpi=120, width=8, height=0.15 * length(levels(sizes.per.day$experiment)) + 1)
}

plot.sizes.per.day <- function(sizes.per.day) {
    p <- ggplot(sizes.per.day) +
        geom_boxplot(aes(x=node, y=count)) +
        ggtitle(paste("Data uploaded from each router per day for experiment", sizes.per.day$experiment)) +
        labs(y="Data uploaded per day", x="Experiment") +
        scale_y_continuous(labels=format_bytes()) +
        coord_flip()

    filename <- paste(output.dir, "/dailyusage-", sizes.per.day$experiment, ".png", sep="")
    ggsave(filename, p, dpi=120, width=8, height=0.15 * length(unique(sizes.per.day$node)) + 1)
}

size.summary <- read.csv(paste(dirname, "size-summary.csv", sep="/"))
interarrival.summary <- read.csv(paste(dirname, "interarrival-times-summary.csv", sep="/"))
interarrival.summary[interarrival.summary$experiment == "mac-analyzer", "X50th"] <- 10
agg.interarrivals <- ddply(interarrival.summary, .(experiment), weight.quantiles)
both.summary <- merge(size.summary, agg.interarrivals, by="experiment", suffixes=c("", ".interarrival"))
sizes.per.day <- read.csv(paste(dirname, "sizes-per-day.csv", sep="/"))

d_ply(size.summary, .(experiment), plot.counts)
d_ply(size.summary, .(experiment), plot.sizes)
plot.all.counts(size.summary)
agg.sizes <- plot.sizes.all.experiments(size.summary)
d_ply(interarrival.summary, .(experiment), plot.interarrivals)
gg.interarrival <- plot.interarrivals.all.experiments(interarrival.summary)
d_ply(both.summary, .(experiment), plot.usage)
plot.usage.all.experiments(size.summary, agg.interarrivals)
plot.sizes.per.day.all.experiments(sizes.per.day)
d_ply(sizes.per.day, .(experiment), plot.sizes.per.day)
