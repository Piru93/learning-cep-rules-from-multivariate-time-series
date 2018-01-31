#############
## OLD CODE

suppressPackageStartupMessages(library(changepoint))
options("digits"=22)
options("scipen"=999)

# # User-defined labels to understand if an event type should be considered as a time series 
# event_type_labels = list("ts",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "point",
#                          "ts",
#                          "ts",
#                          "point",
#                          "point",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "point",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "point",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "point",
#                          "point",
#                          "point",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "ts",
#                          "point"
#                          
# )


for(i in 1:16)
  dimensions[[i]] = c("nodeNumber","jobId")

for(i in 17:47)
  dimensions[[i]] = "nodeNumber"

t <- list()
# Add timestamps to the global set
t[[i]] <- dt[[i]][,ncol(dt[[i]]),with=FALSE]

# We label invalid dimensions as "n"
for (i in 1:l){
  attr <- colnames(dt[[i]])[attr_labels[[i]] == "d"]
  for(a in attr[length(attr):1]){
    if(( a != "nodeNumber" && dt[[i]][,a,with=FALSE][1] == 0) || (a == "nodeNumber" && dt[[i]][,a,with=FALSE][1] == 999))
      attr_labels[[i]][match(a,colnames(dt[[i]]))] = "n"
  }
}

# Parallel reading events from a "file" selecting "attributes"
dt <- list
attributes <- c("nodeNumber","jobId", "taskId", "attemptID", "value", "timestamp")
read_events_parallel <- function(file, attributes){
  d <- fread(file, # Already relevant and partitioning attributes are selected
             sep = ",",
             select = attributes,
             integer64="numeric")
  return(d)
}

for(i in 1:no_files)
  dt[[i]] <- unique(dt[[i]])

dt <- lapply(files,read_events_parallel,c("nodeNumber","jobId", "taskId", "attemptID", "value", "timestamp"))

# Create a correct data.table for timestamps
t <- rbindlist(t)
t <- unlist(t)
t <- unique(t)
t <- sort(t)
t <- data.table(timestamp=x)


# partition_tuples <- function(attr, dt_attr, step, k, part_attr, n_attr){
#   if(step==n_attr+1){
#     tuples[[t]] <<- list()
#     for(j in part_attr)
#       tuples[[t]][[j]] <<- dt_attr[[j]][1]
#     t <<- t+1
#   }
#   else{
#     if(step==1){ ## 1st partition, i.e. step = 0
#       ids <- sort(unique(dt_attr[[attr]]))
#       len <- min(k,length(ids))
#       for(id in head(ids, len)){
#         dt_attr_next <- dt_attr[eval(parse(text=attr)) == id]
#         partition_tuples(part_attr[step+1], dt_attr_next, step+1, k, part_attr, n_attr)
#       }
#     }
#     else{ ## "step-th" partition, step > 0
#       ids <- sort(unique(dt_attr[[attr]]))
#       len <- min(k,length(ids))
#       for (id in head(ids, len)){
#         partition_tuples(part_attr[step+1], dt_attr[eval(parse(text=part_attr[step])) == id], step+1, k, part_attr, n_attr)
#       }
#     }
#   }
# }
# 
# tuples <- list()
# t <- 1
# for(i in 1:l){
#   part_attr <- colnames(dt[[i]])[attr_labels[[i]] == "d"]
#   n_attr <- length(part_attr)
#   depth <- 10
#   # if(length(part_attr)==0){ ## No real partitions
#   #   tuples[[t]] <- list()
#   #   for(j in colnames(dt[[i]])[attr_labels[[i]] == "n"])
#   #     tuples[[t]][[j]] <- dt[[i]][[j]][1]
#   #   t <- t+1
#   # }
#   # else
#   partition_tuples(part_attr[1], dt[[i]], 1, depth, part_attr, n_attr)
# }

# Obtaining partitioning tuples #2 (brute force approach: permutations)
values <- list()
for(i in 1:no_files){
  for(j in dimensions[[i]]){
    if(i == 1){
      values[[j]] <- unlist(unique(dt[[i]][[j]]))
    }
    else
      values[[j]] <- append(values[[j]],unlist(unique(dt[[i]][[j]])),after=length(values[[j]]))
  }
}

for(i in 1:2){
  values[[i]] <- unique(values[[i]])
}

i <- 1
tuples_bis <- list()
for(node in values$nodeNumber)
  for(job in values$jobId){
    tuples[[i]] <- list("nodeNumber"= node,"jobId" = job)
    i <- i+1
  }

# # Parallel version
library(doParallel)
registerDoParallel(cores=4)
system.time({foreach(w=1:4) %dopar% whole_partitions_parallel_to_file(w)})
ptime


# 
# whole_partitions_parallel <- function(index){
#   wP <- list()
#   t <- tuples[index]
#   for(j in 1:length(files)){
#     d <- dt[[j]]
#     if(j == 17 || j == 18 || j == 19){# special case nodeNumber=999, they will be included in any partition
#       wP[[files[j]]] <- d
#       next
#     }
#     part_attr <- dimensions[[j]]
#     for(attr in part_attr)
#       d <- d[eval(parse(text=attr)) == t[[attr]] ] 
#     if(nrow(d) != 0){
#       wP[[files[j]]] <- d
#     }
#   }
#   #print(paste0("partition_",index))
#   return(wP)
# }
# 
whole_partitions_parallel_to_file <- function(index){
  t <- tuples[index]
  dir = paste0("partitions/partition_",index)
  dir.create(dir)
  for(j in 1:length(files)){
    d <- dt[[j]]
    if(j == 17 || j == 18 || j == 19){# special case nodeNumber=999
      #fwrite(d, out, col.names=TRUE, sep=",")
      next
    }
    part_attr <- colnames(dt[[j]])[dimensions[[j]] == "d"]
    for(attr in part_attr)
      d <- d[eval(parse(text=attr)) == t[[attr]] ]
    if(nrow(d) != 0){
      out = paste0(dir,"/part_",files[j])
      data.table::fwrite(d, out, col.names=TRUE, sep=",")
    }
  }
}

# Find the time window for a partition according to the job
find_limit_job <- function(P){
  min = Inf
  max = 0
  for(i in 1:length(P)){
    p <- P[[i]]
    if(match(names(P)[i],files) < 17){ #only look job-specific events
      if(max(p$timestamp) > max)
        max = max(p$timestamp)
      if(min(p$timestamp) < min)
        min = min(p$timestamp)
    }
  }
  return(c(min,max))
}

# Writing some partitions to file and checking consistency of partitioning tuple
p <- list()
for(i in 1:16){
  #p[[files[[i]]]] <- dt[[i]][jobId == job & taskId == task]
  data.table::fwrite(p[[files[[i]]]], "example.csv", col.names=TRUE, sep=",", append=TRUE)
}
p[[files[[2]]]] <- dt[[2]][jobId == job]
p[[files[[3]]]] <- dt[[3]][jobId == job]

# Finding/plotting trends function
find_trend <- function(x, y){
  tau = list()
  j = 1
  i = 1
  W = length(y)
  while(i < W){
    a = x[i]
    s = sign(y[i+1]-y[i])
    repeat{
      i = i+1
      c = sign(y[i+1]-y[i])
      if(!(s==c) || (i==W))
        break
    }
    b = x[i]
    tau[[j]] = list()
    tau[[j]]$sign = s
    tau[[j]]$a = a
    tau[[j]]$b = b
    j = j+1
  }
  return(tau)
}

plot_trend <- function(T, xlim){
  ylim = c(-2,2)
  plot(0,0, t="n", xlim = xlim, ylim = ylim, xlab = "timestamp", ylab = "trend")
  #axis(1,c(min(x):max(x)))
  #plot(x,y, t="n")
  for(tau in T){
    c = "black"
    if(tau$sign == 1)
      c = "blue"
    if(tau$sign == -1)
      c = "red"
    lines(c(tau$a,tau$b), rep(tau$sign,2), t="l", col=c)
  }
}

# Process partitions (plot, trends, change point, ecc)
process_partitions <- function(P){
  for(p in P){
    par(mfrow=c(2,1))
    # Title specifying partitioning attributes and values
    main = ""
    for(i in 1:(ncol(p)-2))
      main = paste(main,colnames(p)[[i]],p[[i]][1])
    main = paste(main,"\nNo. points",nrow(p))

    # Time stamp min / max
    xlim <- c(min(p$timestamp), max(p$timestamp))
    ylim <- c(min(p$value), max(p$value))

    plot(x=xlim[0], y=ylim[0], t = "n", xlim = xlim, ylim = ylim, xlab="timestamp", ylab="value",
         main = main)
    lines(p$timestamp, p$value, col = "black", type="l")

    # Plot trend on the same figure
    T = find_trend(p$timestamp, p$value)
    plot_trend(T, xlim)

    if(nrow(p)!=1){
      par(mfrow=c(3,2))
      # Plot change point detection on the same figure (using PELT and BinSeg)

      # mean
      plot(as.ts(p$value), ylab="value" ,  main="PELT - mean")
      abline(v=cpt.mean(p$value, method="PELT")@cpts, col="red")
      plot(as.ts(p$value), ylab="value" ,  main="Binseg - mean")
      abline(v=cpt.mean(p$value, method="BinSeg")@cpts, col="red")


      # variance
      plot(as.ts(p$value), ylab="value" ,  main="PELT - var")
      abline(v=cpt.var(p$value, method="PELT")@cpts, col="red")
      plot(as.ts(p$value), ylab="value" ,  main="Binseg - var")
      abline(v=cpt.var(p$value, method="BinSeg")@cpts, col="red")

      # meanvar
      plot(as.ts(p$value), ylab="value" ,  main="PELT - meanvar")
      abline(v=cpt.meanvar(p$value, method="PELT")@cpts, col="red")
      plot(as.ts(p$value), ylab="value" ,  main="Binseg - meanvar")
      abline(v=cpt.meanvar(p$value, method="BinSeg")@cpts, col="red")
    }

  }
}

# Obtaining partitions-per-event-type
P = list()
for (i in 1:l){
  P[[i]] = list()
  part_attr <- colnames(dt[[i]])[attr_labels[[i]] == "part"]
  n_attr <- length(part_attr)
  depth <- 1000
  if(length(part_attr)==0){ ## No real partitions
    P[[i]] <- dt[[i]]
  }
  else
    P[[i]] <- partition(part_attr[1], dt[[i]], 1, depth, part_attr, n_attr, P[[i]])
}

# Processing partitions to produce plots
for (i in 1:l){
  if(ncol(dt[[i]])==3){ ## case with nodeNumber
    pdf(paste0(tools::file_path_sans_ext(files[i]),".pdf"))
    process_partitions(P[[i]])
    dev.off()
  }
  else{ ## case with jobId, taskId, attemptId, nodeNumber
    pdf(paste0(tools::file_path_sans_ext(files[i]),".pdf"))
    process_partitions(P[[i]])
    dev.off()
  }
}

## Obtaining partitioning tuples in two different ways

# Building the groupby attributes from the full set of partitions-per-event-type
groupby <- list()
k = 1
for(i in 1:l)
    for(p in P[[i]]){
      groupby[[k]] <- list()
      part_attr <- colnames(dt[[i]])[attr_labels[[i]] == "part"]
      if(length(part_attr)==0){
        part_attr <- colnames(dt[[i]])[attr_labels[[i]] == "nopart"]
        p <- dt[[i]]
      }
      for(j in part_attr)
          groupby[[k]][[j]] <- p[[j]][1]
      k <- k+1
    }

## notice that "tuples" is equivalent to "groupby", they are just two different notations
## the idea is to merge the two partioning functions together to get only one


## PROBLEM: duplicated timestamps and different attribute values. ##
## SOLUTION: Partition them "hierarchically" (e.g. jobId->taskId->attemptId->(value,timestamp) ) ##
## N.B. We don't merge all the timestamps since we use partition (resolution wouldn't be useful for visualization)

# Checking which attributes have unique values (jobId, taskId, attemptID, nodeNumber, timestamp, value)
# as to create our partitioning attributes
d = list()
attr = list()
k = 0
for (i in 1:l){
  for(j in 1:ncol(dt[[i]])){
    name <- colnames(dt[[i]])[j]
    if(!is.element(name,attr)){
      k <- k+1;
      attr[[k]] <- name
      d[[name]] = list()
      d[[name]][["name"]] <- name
      d[[name]][["unique"]] <- 0
      d[[name]][["total"]] <- 0
      d[[name]][["files"]] <- list()
      d[[name]][["files"]] <- vector(mode="raw")
    }
    d[[name]][["total"]] <- d[[name]][["total"]] + 1
    s <- nrow(unique(dt[[i]][,j,with=FALSE]))
    if(s==1){
      d[[name]][["unique"]] <- d[[name]][["unique"]]+1
      d[[name]][["files"]]<- append(d[[name]][["files"]],files[i])
    }
  }
}

for(i in 1:k)
  if(d[[i]]["unique"][[1]]!=d[[i]]["total"][[1]])
    print(d[[i]]["name"])

for(i in 1:l){
  if(ncol(dt[[i]])>3)
    attr_labels[[i]] <- c("part","part","part","part","cont","t")
  else
    attr_labels[[i]] <- c("part","cont","t")
}

# Create timeseries (value,timestamp) for each partition, manually
for (i in 1:l){
  pdf(paste0(tools::file_path_sans_ext(files[i]),".pdf"))
  if(ncol(dt[[i]])==3){ ## case with nodeNumber, value, timestamp
    nodeNumbers <- sort(unique(dt[[i]]$nodeNumber))
    rainbow_ <- rainbow(length(nodeNumbers))

    for (j in 1:length(head(nodeNumbers,n=10))){
      node <- nodeNumbers[j]
      dt_node <- dt[[i]][nodeNumber == node, .(timestamp, value)]
      dt_node <- dt_node[!duplicated(dt_node)]

      ## Merge with all timestamps
      # dt_node <- merge(t,dt_node,by="timestamp",all=TRUE,allow.cartesian=TRUE)


      # - Time stamp min / max
      xlim <- c(min(dt_node$timestamp), max(dt_node$timestamp))
      ylim <- c(min(dt_node$value), max(dt_node$value))

      plot(x=xlim[0], y=ylim[0], t = "p", xlim = xlim, ylim = ylim, xlab="timestamp", ylab="value",
           main = paste("nodeNumber", node))

      lines(dt_node$timestamp, dt_node$value, col = rainbow_[j])
    }
    dev.off()
  }
  else{ ## case with jobId, taskId, attemptId, value, timestamp
    jobIds <- sort(unique(dt[[i]]$jobId))
    for (jobId_ in head(jobIds,n=3)) {
      dt_job <- dt[[i]][jobId == jobId_, .(timestamp, value)]

      ## Merge with all timestamps
      # dt_job <- merge(t,dt_job,by="timestamp",all=TRUE,allow.cartesian=TRUE)

      # - Time stamp min / max
      xlim <- c(min(dt_job$timestamp), max(dt_job$timestamp))
      ylim <- c(min(dt_job$value), max(dt_job$value))

      plot(x=xlim[0], y=ylim[0], t = "p", xlim = xlim, ylim = ylim, xlab="timestamp", ylab="value",
           main = paste("JobId", jobId_))

      taskIds <- sort(unique(dt[[i]][jobId == jobId_, taskId]))
      rainbow_ <- rainbow(length(taskIds))

      for (i_taskId in 1:length(head(taskIds,n=4))){
        taskId_ <- taskIds[i_taskId]
        attemptIDs <- sort(unique(dt[[i]][jobId == jobId_ & taskId == taskId_, attemptID]))
        for (attemptID_ in head(attemptIDs,5)) {
          job_task_attempt <- dt[[i]][jobId == jobId_ & taskId == taskId_ & attemptID == attemptID_][order(timestamp)]

          # - Make sure that measurement is unique
          jta_unique <- job_task_attempt[, lapply(.SD, function(x)  {
            u <- unique(x)
            if (length(u) == 1) {
              return(u)
            } else {
              return(NA_real_)
            }
          }), by="timestamp", .SDcols="value"]

          lines(jta_unique$timestamp, jta_unique$value, col = rainbow_[i_taskId])
        }

      }

    }

    dev.off()
  }

}

library(doParallel)
cl <- makeCluster(2)
#registerDoParallel(cl)
#foreach(w=1:2) %dopar% whole_partitions_parallel(tuples, w, dt, attr_labels, files)
registerDoParallel(cl)
clusterExport(cl, varlist=list("tuples","dt", "attr_labels", "files"))
w = 1:2
parallel::parLapply(cl=cl,w,whole_partitions_parallel)

# # Start up a parallel cluster
# parallelCluster <- parallel::makeCluster(parallel::detectCores())
# if(!is.null(parallelCluster)) {
#   parallel::stopCluster(parallelCluster)
#   parallelCluster <- c()
# }
# 
# # Build the single argument function we are going to pass to parallel
# mkWorker <- function(t,dt,no_files,attr_labels) {
#   force(dt)
#   force(no_files)
#   force(attr_labels)
#   whole_partitions_parallel <- function(t,dt,no_files,attr_labels){
#     wP = list()
#     for(i in 1:no_files){
#       d <- dt[[i]]
#       part_attr <- colnames(dt[[i]])[attr_labels[[i]] == "part"]
#       # A type may belong to different partitions if the tuple does not match entirely the set of "part" attributes
#       # if the match is a "left-to-right subset" it is retained
#       # positive example (jobId) in (jobId,taskId)
#       # negative  example (taskId) in (jobId,taskId)
#       p = 0
#       while(p<length(part_attr) && is.element(part_attr[p+1], names(t)))
#         p = p+1
#       if(p==0) # no "left-to-right" subset
#         # (**OPEN PROBLEM: what about nodeNumber=999 and no other attributes. Which partition?? for the moment no one**)
#         next
#       for(attr in part_attr[1:p])
#         d <- d[eval(parse(text=attr)) == t[[attr]]]
#       if(nrow(d) != 0){
#         wP[[files[i]]] <- d
#       }
#     }
#     return(wP)
#   }
#   worker <- function(t) {
#     whole_partitions_parallel(t,dt,no_files,attr_labels)
#   }
#   return(worker)
# }
# 
# wP_bis <- list(length(100))
# tryCatch(
# wP_bis <- parallel::parLapply(parallelCluster,head(tuples,100),mkWorker(t,dt,no_files,attr_labels)),
# error = function(e) print(e)
# )

# Find the (inverse of the) mean gap for an event type into a partition
# The inverse because of Inf values for pointwise events; in this way mean_gap(POINT) -> 0
mean_gap <- function(p){
  sum <- 0
  y = p$timestamp
  if(length(y)==1)
    #return(.Machine$double.xmax)
    return(0)
  for(i in 2:length(y))
    sum <- sum + y[i]-y[i-1]
  #return(sum/(length(y)-1))
  return((length(y))^2/sum)
}


j=1
freq <- list()
for(P in wP){
  freq[[j]] <- vector(mode="numeric",length=length(P))
  freq[[j]] <- lapply(P,mean_gap) # we use the average gap between observations to derive labels
  #pdf(paste0("partition_",j,".pdf"))
  #xlim <- find_limit(P)
  # i=1
  # for(p in P){
  #   x <- p$timestamp
  #   y <- p$value
  #   plot(y=y,x=x, main=paste0(tools::file_path_sans_ext(names(P[i]))," No.points ",length(y)), type="p", xlim=xlim, xlab="timeStamp",ylab = "value")
  #   i <- i+1
  # }
  #dev.off()
  j <- j+1
}


# Label types in a partition
classify <- function(f, method){
  f <- unlist(f)
  labels <- list()
  # case in which there are no 3 different classes
  if(length(unique(f))<3){
    min = min(unlist(f))
    max = max(unlist(f))
    m = c(min,max)
    for(i in 1:length(f))
      if(which.min(abs(m-f[i]))==1)
        labels[[i]] = "point"
    else
      labels[[i]] = "ts"
    return(labels)
  }
  if("method"=="kmeans")
    fit <- kmeans(f,3)
  else
    fit <- Ckmeans.1d.dp(f,3)
  min = min(fit$centers)
  max = max(fit$centers)
  m <- c(min,max)
  for(i in 1:length(f)){
    if(which.min(abs(m-f[i]))==1)
      labels[[i]] = "point"
    else
      labels[[i]] = "ts"
  }
  return(labels)
}


# Label all partitions in two different ways
#labels_km <- lapply(freq,classify,"kmeans")
labels_ck <- lapply(freq,classify,"ckmeans")


# Plot clusters (in both ways) for a random partition
for(w in 1:5){
  # par(mfrow=c(2,1))
  # y = freq[[w]]
  # x = 1:length(freq[[w]])
  # plot(y=y ,x=x, col=ifelse(labels_ck[[w]][x] == "ts" ,'blue','red'))
  # plot(y=y ,x=x, col=ifelse(labels_km[[w]][x] == "ts" ,'blue','red'))
  f <- unlist(freq[[w]])
  g <- getJenksBreaks(f,2)
  plot(x=f,y=1:length(f),type="h")
  abline(v=g, col="red")
}


# Debugging some partitions plot (labels, sax, etc)
for(w in 1:10){
  P <- wP[[w]]
  par(mfrow=c(1,1))
  xlim <- find_limit(P)
  i=1
  dir = paste0("partition_with_labels/")
  dir.create(dir)
  for(p in P){
    x <- p$timestamp
    y <- p$value
    type = tools::file_path_sans_ext(names(P)[i])
    jpeg(paste0(dir,"/",type,".jpg"))
    plot(y=y, x=x,
         col=ifelse(labels_ck[[w]][i] == "ts" ,'blue','red'),
         main=paste0(type," No.points: ",length(y)," Label: ",labels_ck[[w]][i]),
         xlim = xlim,
         type="p", xlab="timestamp", ylab = "value")
    
    dev.off()
    i <- i+1
  }
}


# Check which is the smallest granularity needed to plot a time series:
# what is the maximum number of attributes which is shared by points with same timestamp
# for(i in 1:47){
#   d_timestamp <- dt[[i]][duplicated(dt[[i]]$timestamp)]$timestamp # duplicated timestamps
#   #rows <- dt[[i]][is.element(timestamp,t)] # rows with such timestamps
#   min = 1
#   for(t in d_timestamp){
#     rows <- dt[[i]][timestamp == t] # rows with a certain duplicated timestamp
#     count <- 1
#     for(d in 1:length(dimensions[[i]])){ # check how many attributes are equal for each duplicated timestamp
#       if(anyDuplicated(rows[[1:d]]) > 1)
#         count <- count + 1
#       else
#         break
#     }
#     if(count > min)
#       min = count
#   }
# }

# Double check if we could not partition by (Node,Job) only
# for(i in 1:16){
#   d_timestamp <- dt[[i]][duplicated(dt[[i]]$timestamp)]$timestamp
#   rows <- dt[[i]][is.element(timestamp,t) && anyDuplicated(rows[[1:3]])>1]
#   print(rows)
# }

library(devtools)
library(jmotif)

# # Playing with segmentation
# w <- sample(1:length(wP),1)
# for(w in 1:10){
#   P <- wP[[w]]
#   par(mfrow=c(1,1))
#   xlim <- find_limit_job(P)
#   i=1
#   for(p in P){
#     x <- p[timestamp >= xlim[1] & timestamp <= xlim[2]]$timestamp
#     y <- p[timestamp >= xlim[1] & timestamp <= xlim[2]]$value
#     type = tools::file_path_sans_ext(names(P)[i])
#     if(!is.null(p$taskId[1]))
#       no_tasks <- length(unique(p$taskId))
#     else
#       no_tasks <- 0
#     plot(y=y, x=x, xlim=xlim,
#          main=paste0(type," nodeNumber: ", p$nodeNumber[1], " jobId: ", p$jobId[1]),
#          sub=paste0(" No.points: ",length(y)," No.tasks: ", no_tasks),
#          col=c,
#          type="p", xlab="timestamp", ylab = "value")
#     i <- i+1
#   }
# }

# INPUT: HadoopDataActivity (first 1000 points)
p <- P$`partitions/partition_1/part_1_HadoopDataActivity`$value[1:1000]
plot(p,type="o",ylab="value",xlab="time")
for(w in c(3,4,5,7,10,20,50,100)){
  # build_trends_window(p,w)
  # build_trends_window_max(p,w,2*w)
  # build_trends_bottom_up(p,w)
  # build_trends_bottom_up_opt(p,w)
  # build_trends_bottom_up_opt_max(p,w,2*w)
  # build_trends_window_max_merge(p,w,2*w)
  # build_trends_SWAB(p,w,4*w)
}
# INPUT: bytes_in (first 2000 points)
p <- P$`partitions/partition_1/part_1_bytes_in_event`$value[1:2000]
plot(p,type="o",ylab="value",xlab="time")
for(w in c(3,4,5,7,10,20,50,100)){
  # build_trends_window(p,w)
  # build_trends_window_max(p,w,2*w)
  # build_trends_bottom_up(p,w)
  # build_trends_bottom_up_opt(p,w)
  # build_trends_bottom_up_opt_max(p,w,2*w)
  # build_trends_window_max_merge(p,w,2*w)
  # build_trends_SWAB(p,w,4*w)
}

# INPUT: Monthly Average Heights of the Rio Negro river at Manaus (1080 points)
p <- as.numeric(manaus)
plot(p,type="o",ylab="value",xlab="time")
for(w in c(3,4,5,7,10,20,50,100)){
  # build_trends_window(p,w)
  # build_trends_window_max(p,w,2*w)
  # build_trends_bottom_up(p,w)
  # build_trends_bottom_up_opt(p,w)
  # build_trends_bottom_up_opt_max(p,w,2*w)
  # build_trends_window_max_merge(p,w,2*w)
  # build_trends_SWAB(p,w,4*w)
}

library(boot)
# INPUT: Australian Relative Wool Prices (309 points)
p <- as.numeric(wool)
plot(p,type="o",ylab="value",xlab="time")
for(w in c(3,4,5,7,10,20,50,100)){
  build_trends_window(p,w)
  build_trends_window_max(p,w,2*w)
  build_trends_bottom_up(p,w)
  build_trends_bottom_up_opt(p,w)
  build_trends_bottom_up_opt_max(p,w,2*w)
  build_trends_window_max_merge(p,w,2*w)
  build_trends_SWAB(p,w,4*w)
}

library(DAAG)
p <- SP500close
# test with the latest implementation of the trend segmentation
trends <- list()
trends <- build_trends_bottom_up_opt_max(p,20,300)
trends <- build_trends_window_max_merge(p,100,500)
trends <- build_trends_SWAB_merge(p,50,300)
plot_trends(trends,"example","method")

# tests with breakpoints, ggplot2, others
library(strucchange)
y <- p[1:500]
x=1:500
bp<-breakpoints(formula=y ~ x, h = 10)
plot(bp)
plot(x, y,type="o")
lines(x, fitted(bp, breaks = 10), col = 2, lwd = 2)
lines(x, fitted(bp, breaks = 20), col = 4, lwd = 2)

options(scipen=999)


library(ggplot2)
p <- P$`partitions/partition_1/part_1_HadoopDataActivity`$value[500:1000]
p <- P$part_425_MapPeriod$value
plot(p,ylab="value")
qplot(y=x, x=1:length(x), geom="line", xlab="time", ylab="value")+
  geom_vline(xintercept=cpts@cpts[1], color="red", linetype="dashed")+
  geom_vline(xintercept=cpts@cpts[2], color="blue", linetype="dashed")+
  geom_vline(xintercept=cpts@cpts[3], color="green", linetype="dashed")

############### Mann-Kendall function ----------

compute_tied_all <- function(x){
  groups <- list()
  g <- 1
  for(i in x){
    index <- as.character(i)
    if(is.null(groups[[index]]))
      groups[[index]] <- 1
    else
      groups[[index]] <- groups[[index]]+1
  }
  return(groups)
}
add_tied <- function(x, groups){
  index <- as.character(i)
  if(is.null(groups[[index]]))
    groups[[index]] <- 1
  else
    groups[[index]] <- groups[[index]]+1
  return(groups)
}

library(Rcpp)
cppFunction(
  "IntegerVector CDcount(NumericVector x, NumericVector y) {
  IntegerVector counts(2, 0);
  int n = x.size();
  for (int i=0; i < n; ++i) {
  for (int j=i+1; j < n; ++j) {
  counts[0] += (x[i] < x[j]) && (y[i] < y[j]);
  counts[1] += (x[i] < x[j]) && (y[i] > y[j]);
  }
  }
  return counts;
  }")

pairs <- function(x, y) {
  n <- length(x)
  ix <- order(x)
  x <- x[ix]
  y <- y[ix]
  counts <- CDcount(x, y)
  return(list(Nc=counts[1], Nd=counts[2]))
}
y <- as.integer(runif(10000,0,1000))
x <- 1:10000

optimized_mk <- function(y){
  p <- pairs(y,1:length(y))
  S <- p$Nc - p$Nd
  tied <- compute_tied_all(y)
  n_tied <- 0
  for(t in tied){
    n_tied <- n_tied + t*(t-1)*(2*t+5)
  }
  n <- length(y)
  varS <- 1/18*(n*(n-1)*(2*n+5) - n_tied)
  Z <- sign(S)*(abs(S)-1)/sqrt(varS)
  # if(S > 0){
  #   z <- (s-1)/sqrt(varS)
  # }
  # if(S < 0){
  #   z <- (s+1)/sqrt(varS)
  # }
  # if(S == 0)
  #   z <- 0
  return(list(S=S, varS=varS, Z=Z, pvalue = 2*pnorm(-abs(Z))))
}

system.time(optimized_mk(y))
system.time(mk.test(y))
system.time(MannKendall(y))

nodes <- unique(dt[[20]]$nodeNumber)
for(i in g){
  name = tools::file_path_sans_ext(files[[i]])
  name <- basename(name)
  y <- dt[[i]][nodeNumber == 1]$value
  plot(y,main=name,type="l")
  # for(n in nodes){
  #   jpeg(paste0(name,n,".jpg"))
  #   main = paste0("Event type:", name," - Node: ",n)
  #   y <- dt[[i]][nodeNumber == n]$value
  #   plot(y,main=main)
  #   # if(length(y)>0){
  #   #   if(MannKendall(y)$sl>0.05){
  #   #     write(main,file="file.txt", append=TRUE)
  #   #   }
  #   # }
  #   dev.off()
  # }
}
for(i in 17:19){
  name = tools::file_path_sans_ext(files[[i]])
  name <- basename(name)
  jpeg(paste0(name,"999.jpg"))
  main = paste0("Event type:", name," - Node: ",999)
  y <- dt[[i]][nodeNumber == 999]$value
  plot(y,main=main,type="l")
  dev.off()
  # if(length(y)>0){
  #   if(MannKendall(y)$sl>0.05){
  #     write(main,file="file.txt", append=TRUE)
  #   }
  # }
}

# ## test running time for algorithms
# # hint: generate 1000 subsequences of different sizes from a given time series and average results
# {
# results <- list()
# y<-SP500close
# for(i in 1:1000){
#   for(size in c(100,500,1000,2000)){
#     p <- y[1:size]
#     for(alpha in c(0.01,0.02,0.05)){
#       for(w in c(5,7,10,20)){
#         i <- 1
#         main = paste0(size,"_",alpha,"_",w)
#         results[[main]] <- list()
#         results[[main]][[i]] <- system.time(build_trends_window(p,w, alpha))
#         i <- i+1
#         results[[main]][[i]] <- system.time(build_trends_window_max(p,w,2*w, alpha))
#         i <- i+1
#         results[[main]][[i]] <- system.time(build_trends_window_max_merge(p,w,2*w, alpha))
#         i <- i+1
#         results[[main]][[i]] <- system.time(build_trends_bottom_up(p,w, alpha))
#         i <- i+1
#         results[[main]][[i]] <- system.time(build_trends_bottom_up_opt(p,w, alpha))
#         i <- i+1
#         results[[main]][[i]] <- system.time(build_trends_bottom_up_opt_max(p,w,2*w, alpha))
#         i <- i+1
#         results[[main]][[i]] <- system.time(build_trends_SWAB(p,w,5*w, alpha))
#         i <- i+1
#         results[[main]][[i]] <- system.time(build_trends_SWAB_merge(p,w,5*w, alpha))
#       }
#     }
#   }
# }
# for(size in c(100,500,1000,2000)){
#   pdf(paste0("size_",size,".pdf"))
#   for(alpha in c(0.01,0.02,0.05)){
#     for(w in c(5,7,10,20)){
#       main = paste0(size,"_",alpha,"_",w)
#       i <- 1
#       y <- vector(length=8)
#       for(r in results[[main]]){
#         y[i]=r[3]
#         i <- i +1
#       }
#       plot(y, x=1:8, xlab="win  wmax  wmerge  bot  botopt  botoptmax  swab  swabm", main=paste0("Alpha-W: ",main), type="h")
#     }
#   }
#   dev.off()
# }
# }

# pdf(paste0("example.pdf"))
# for(ts in ts_list){ # loop on different time series
#   name <- ts$name
#   #stats[[name]] <- list()
#   p <- ts$value
#   if(length(p)>0)
#   plot(p,main=name)
#   # pdf(paste0(name,".pdf"))
#   # for(i in 1:(length(p)/1000)){
#   #   a = 1000*(i-1)
#   #   b = 1000*i-1
#   #   plot(p[a:b])
#   # }
#   # dev.off()
# }
# dev.off()

########### Mann-Kendall test on Hadoop time series: checking relevant time series for algorithm evaluation ---------

## In GMOND we have 27 event types, some of which are "pointwise"
## We have P nodeNumber values, thus P partitions (P < 10)
## For each event type T (T < 27) we take one time series from one partition
## and we evaluate our segmentation algorithms on this sub-sample of time series
## The evaluation is performed by taking S sub-sequences of the entire time series (length(S_i) = 500, S < 10)

# different nodeNumber
nodes <- unique(dt[[21]]$nodeNumber)

# Ganglia event types
gmond <- 20:47
# Ganglia event types which are pointwise (indexes)
not_contin_gmond <- c(47, 40, 39, 32, 27, 26, 25, 23, 20, 19)
# Ganglia continuous event types
gmond <- gmond[!is.element(gmond,not_contin_gmond)]

# Taking sample time series from GMOND
gmond_ts <- list()
i <- 1
for(g in gmond){
  gmond_ts[[i]] <- list()
  name = tools::file_path_sans_ext(files[[g]])
  name <- basename(name)
  rm(.Random.seed)
  n <- sample(nodes, size=1)
  gmond_ts[[i]][["value"]] <- dt[[g]][nodeNumber == n]$value
  gmond_ts[[i]][["name"]] <- name
  i <- i+1
}
gmond_ts[[i]] <- list()
gmond_ts[[i]][["value"]] <- dt[[17]]$value
gmond_ts[[i]][["name"]] <- "balance-numeric"
gmond_ts[[i+1]] <- list()
gmond_ts[[i+1]][["value"]] <- dt[[18]]$value
gmond_ts[[i+1]][["name"]] <- "balance-standarddeviation"


## In Hadoop we have only 3 relevant event types for segmentation (HadoopDataActivity, MapPeriod and PullPeriod)
## We take 10 sub-samples of all the possible time series (535 partitions = 535 time series) for each of them and evaluate algorithms

# Hadoop event types which are continuous
hadoop <- c(1,5,10)

# Selecting partitions containing those event types
partitions <- list()
for(i in hadoop){ # only Hadoop events
  name <- as.character(i)
  partitions[[name]] <- list()
  d <- dt[[i]]
  rows <- dt[[i]][, dimensions[[i]],with=FALSE]
  partitions[[name]] <- unique(rows)
}

hadoop_ts <- list()
k <- 1
# plotting 10 time series for event type from 10 different partitions
for(i in 1:100){
  for(h in hadoop){
    tuples <- partitions[as.character(h)]
    rm(.Random.seed)
    j <- sample(1:nrow(tuples[[1]]),1)
    t <- tuples[[1]][j]
    name = tools::file_path_sans_ext(files[[h]])
    name <- basename(name)
    n <- t$nodeNumber
    j <- t$jobId
    # main = paste0("Event type:", name," - Node: ",n, " - Job",j)
    y <- dt[[h]][nodeNumber == n & jobId == j]$value
    if(length(y)>0){
      hadoop_ts[[k]] <- list()
      # jpeg(paste0(name,i,".jpg"))
      # plot(y=y, x=1:length(y))
      # dev.off()
      if(name == "HadoopDataActivity"){ # split in positive and negative trace
        hadoop_ts[[k]][["name"]] <- paste0(name,"_positive")
        hadoop_ts[[k]][["value"]] <- y[y>=0]
        k <- k+1
        hadoop_ts[[k]] <- list()
        hadoop_ts[[k]][["name"]] <- paste0(name,"_positive")
        hadoop_ts[[k]][["value"]] <- y[y<0]
        k <- k+1
      }
      else{
        hadoop_ts[[k]][["name"]] <- name
        hadoop_ts[[k]][["value"]] <- y
        k <- k+1
      }
    }
  }
}

################# Testing algorithm evaluation ------------
## Measures:
## 1) avg_pvalue
## 2) time
## 3) no_trends
## 4) avg_length
## 5) ?

ts_list <- gmond_ts

compute_stats <- function(trends, time){
  sum <- 0
  avg_length = 0
  # zscore <- 0
  pvalue <- 0
  for(t in trends){
    pvalue <- pvalue + t$test$sl
    avg_length <- avg_length + (t$index[2]-t$index[1])
    # zscore <- sign(t$test$S)*(abs(t$test$S)-1)/sqrt(t$test$varS) + zscore
  }
  return(list("avg_pvalue"=pvalue/length(trends), 
              "time"=time, "no_trends"=length(trends),
              "avg_length"=avg_length/length(trends)))
  # "zscore"=zscore/length(trends)))
}
evaluate_performances <- function(ts_list, alphas, wins){
  stats <- list()
  for(a in alphas){
    stats[[as.character(a)]]
    for(w in wins){ # loop on different values for w
      stats[[as.character(a)]][[as.character(w)]] <- list()
      for(ts in ts_list){ # loop on different time series
        name <- ts$name
        stats[[as.character(a)]][[as.character(w)]][[name]] <- list()
        p <- ts$value
        
        # sampling each 10 observations to reduce time series size
        # if(length(p)>500){
        #   p <- p[seq(1,length(p),10)]
        # }
        #main <- paste0(name,"_win",w,"_alpha",alpha)
        #pdf(paste0(main,".pdf"))
        
        ## running algorithms and collecting stats according to compute_stats()
        
        time <- system.time(trends <- build_trends_window(p, w, a))
        stats[[as.character(a)]][[as.character(w)]][[name]][["window"]] <- compute_stats(trends, time)
        # if(length(trends)>0)
        #plot_trends(p, trends,"window", alpha)
        
        for(M in c(2,3,4,5)){
          time <- system.time(trends <- build_trends_window_max(p,w,M*w, a))
          stats[[as.character(a)]][[as.character(w)]][[name]][[paste0("window_max_",M)]] <- compute_stats(trends,time)
          # if(length(trends)>0)
          #plot_trends(p, trends,paste0("window_max_",M), alpha)
          
          time <- system.time(trends <- build_trends_window_max_merge(p,w,M*w, a))
          stats[[as.character(a)]][[as.character(w)]][[name]][[paste0("window_max_merge_",M)]] <- compute_stats(trends,time)
          # if(length(trends)>0)
          #plot_trends(p, trends,paste0("window_max_merge_",M), alpha)
        }
        
        # time <- system.time(trends <- build_trends_bottom_up(p,w, alpha))
        # stats[[w]][[name]][["bottom_up"]] <- compute_stats(trends, time)
        # if(length(trends)>0)
        #   plot_trends(p, trends,"bottom_up", alpha)
        # 
        # time <- system.time(trends <- build_trends_bottom_up_opt(p,w, alpha))
        # stats[[w]][[name]][["bottom_up_opt"]] <- compute_stats(trends, time)
        # if(length(trends)>0)
        #   plot_trends(p, trends,"bottom_up_opt", alpha)
        
        # time <- system.time(trends <- build_trends_bottom_up_opt_max(p,w,3*w, alpha))
        # time <- time + system.time(trends <- merge_trends(p, trends, alpha))
        # stats[[w]][[name]][["bottom_up_opt_max"]] <- compute_stats(trends, time)
        # if(length(trends)>0)
        #   plot_trends(p, trends,"bottom_up_opt_max", alpha)
        
        for(M in c(2,3,4,5)){
          time <- system.time(trends <- build_trends_SWAB(p,w,M*w, a))
          stats[[as.character(a)]][[as.character(w)]][[name]][[paste0("SWAB_",M)]] <- compute_stats(trends, time)
          # if(length(trends)>0)
          #plot_trends(p, trends,paste0("SWAB_",M), alpha)
          
          time <- system.time(trends <- build_trends_SWAB_merge(p,w,M*w, a))
          stats[[as.character(a)]][[as.character(w)]][[name]][[paste0("SWAB_merge_",M)]] <- compute_stats(trends, time)
          # if(length(trends)>0)
          #plot_trends(p, trends,paste0("SWAB_merge_",M), alpha)
        }
        
        #dev.off()
      }
    }
  }
  return(stats)
}

result <- synthetic(1)
ts_list <- list()
i <- 1
for(r in result){
  ts_list[[i]] <- list()
  ts_list[[i]][["name"]] <- paste0("example_",i)
  ts_list[[i]][["value"]] <- r$samples
  i <- i+1
}
stats <- evaluate_performances(ts_list, c(0.05), c(10))

for(s in stats$`0.05`$`10`$example_1){
  print(as.numeric(s$avg_pvalue))
}

avg_stats <- list()
for(a in alphas){
  for(w in wins){
    for(s in stats[[as.character(a)]][[as.character(w)]]){
      for(m in names(s)){
        if(is.null(avg_stats[[m]]))
          avg_stats[[m]] <- c(0,0)
        avg_stats[[m]][1] <- s[[m]]$`sum(pvalue)/no_trends` + avg_stats[[m]][1]
        avg_stats[[m]][2] <- s[[m]]$`sum(sqrt(pvalue))/no_trends` + avg_stats[[m]][2]
      }
    }
  }
}

for(a in avg_stats){
  a[1] <- a/(length(alphas)*length(wins)*length(ts_list))
  a[2] <- a/(length(alphas)*length(wins)*length(ts_list))
}


## anomaly check plots
j <- 2015031422290042
t_start <- 1428034081597
t_end <- 1428035910380
for(g in gmond){
  name = tools::file_path_sans_ext(files[[g]])
  name <- basename(name)
  for(n in tuples[jobId == j]$nodeNumber){
    jpeg(paste0(name,"_",n,"anom.jpg"))
    p <- dt[[g]][timestamp >= t_start-100000 & timestamp <= t_end+100000 & nodeNumber==n]
    #p <- dt[[g]][nodeNumber==n]
    y <- p$value
    x <- p$timestamp
    plot(x=x,y=y,main=paste0(name," - Node: ",n))
    abline(v=c(t_start,t_end))
    dev.off()
  }
}

j2 <- 2015031422290009
t_start <- 1427577885465
t_end <- 1427584304831
for(g in gmond){
  name = tools::file_path_sans_ext(files[[g]])
  name <- basename(name)
  for(n in tuples[jobId == j2]$nodeNumber){
    jpeg(paste0(name,"_",n,".jpg"))
    p <- dt[[g]][timestamp >= t_start-100000 & timestamp <= t_end+100000 & nodeNumber==n]
    #p <- dt[[g]][nodeNumber==n]
    y <- p$value
    x <- p$timestamp
    plot(x=x,y=y,main=paste0(name," - Node: ",n))
    abline(v=c(t_start,t_end))
    dev.off()
  }
}

## Trying different algorithms with different minimum (trend) size and different inputs
## Australian Relative Wool Prices 
p<-wool
for(alpha in c(0.01,0.02,0.05)){
  for(w in c(5,7,10,20,50,100)){
    pdf(paste0("wool_",w,"_",alpha,".pdf"))
    trends <- build_trends_window(p,w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","window", alpha)
    
    trends <- build_trends_window_max(p,w,2*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","window_max", alpha)
    
    trends <- build_trends_bottom_up(p, w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","bottom_up", alpha)
    
    trends <- build_trends_bottom_up_opt(p, w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","bottom_up_opt", alpha)
    
    trends <- build_trends_bottom_up_opt_max(p,w,2*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","bottom_up_opt_max", alpha)
    
    trends <- build_trends_window_max_merge(p,w,2*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","window_max_merge", alpha)
    
    trends <- build_trends_SWAB(p,w,5*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","SWAB", alpha)
    
    trends <- build_trends_SWAB_merge(p,w,5*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","SWAB_merge", alpha)
    
    # trends <- build_trends_experimental(p, w, alpha)
    # if(length(trends)>0)
    #   plot_trends(trends,"example","experimental", alpha)
    
    dev.off()
  }
}

## Closing Numbers for S and P 500 Index 
p<-SP500close
for(alpha in c(0.01,0.02,0.05)){
  for(w in c(5,7,10,20,50,100)){
    pdf(paste0("SP500close_",w,"_",alpha,".pdf"))
    trends <- build_trends_window(p,w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","window", alpha)
    
    trends <- build_trends_window_max(p,w,2*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","window_max", alpha)
    
    trends <- build_trends_window_max_merge(p,w,2*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","window_max_merge", alpha)
    
    trends <- build_trends_bottom_up(p,w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","bottom_up", alpha)
    
    trends <- build_trends_bottom_up_opt(p,w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","bottom_up_opt", alpha)
    
    trends <- build_trends_bottom_up_opt_max(p,w,2*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","bottom_up_opt_max", alpha)
    
    trends <- build_trends_SWAB(p,w,5*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","SWAB", alpha)
    
    trends <- build_trends_SWAB_merge(p,w,5*w, alpha)
    if(length(trends)>0)
      plot_trends(p, trends,"example","SWAB_merge", alpha)
    
    # trends <- build_trends_experimental(p, w, alpha)
    # if(length(trends)>0)
    #   plot_trends(trends,"example","experimental", alpha)
    
    dev.off()
  }
}

## Test with multiple_trend_plot
{
  alpha <- 0.05
  w <- 5
  trends <- build_trends_window(p,w, alpha)
  if(length(trends)>0)
    multiple_plot_trends(trends,"window", alpha)
  
  trends <- build_trends_window_max(p,w,2*w, alpha)
  if(length(trends)>0)
    multiple_plot_trends(trends,"window_max", alpha)
  
  pdf("example.pdf")
  trends <- build_trends_window_max_merge(p,w,2*w, alpha)
  if(length(trends)>0)
    multiple_plot_trends(trends,"window_max_merge", alpha)
  dev.off()
  
  trends <- build_trends_bottom_up(p, w, alpha)
  if(length(trends)>0)
    multiple_plot_trends(trends,"bottom_up", alpha)
  
  trends <- build_trends_bottom_up_opt(p, w, alpha)
  if(length(trends)>0)
    multiple_plot_trends(trends,"bottom_up_opt", alpha)
  
  trends <- build_trends_bottom_up_opt_max(p,w,2*w, alpha)
  if(length(trends)>0)
    multiple_plot_trends(trends,"bottom_up_opt_max", alpha)
  
  trends <- build_trends_SWAB(p,w,5*w, alpha)
  if(length(trends)>0)
    multiple_plot_trends(trends,"SWAB", alpha)
  
  trends <- build_trends_SWAB_merge(p,w,5*w, alpha)
  if(length(trends)>0)
    multiple_plot_trends(trends,"SWAB_merge", alpha)
}

## function to search existing patterns (looping on the entire list)
#search_pattern <- function(patterns, candidate){
#   flag = 0
#   if(length(patterns)==1){
#     if(identical(patterns$relations,candidate)==TRUE)
#       flag = 1
#   }
#   else{
#     i <- 1
#     for(p in patterns){
#       if(identical(p$relations,candidate)==TRUE){
#         flag = i
#         break
#       }
#       i<-i+1
#     }
#   }
#   return(flag)
# }
############### Changepoint analysis  ----------------------------
library(changepoint)
library(changepoint.np)
library(wbs)
library(fpop)

data <- P$`partitions/partition_1/part_1_HadoopDataActivity`$value[1:1000]
data <- as.numeric(data)

plot(data, type="l")
fit <- cpt.mean(data, method="PELT", penalty="SIC")
abline(v=fit@cpts, col="red")

plot(data, type="l")
fit <- cpt.mean(data, method="BinSeg", penalty="SIC")
abline(v=fit@cpts, col="red")

plot(data, type="l")
fit <- cpt.np(data, penalty="SIC", nquantiles=4*log(length(data)))
abline(v=fit@cpts, col="red")

plot(data, type="l")
fit <- wbs(data, 5000)
plot(fit)

#secondDerivative[i] = x[i+1] + x[i-1] - 2 * x[i]