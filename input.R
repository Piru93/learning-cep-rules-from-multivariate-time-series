suppressPackageStartupMessages(library(data.table))
## Read list of events (i.e. files)
files <- (Sys.glob("*.csv"))
no_files = length(files)

## Read each event type (i.e. each ".csv" file) and build a data.table for each one,
## selecting only relevant attributes (user-defined)
dt <- list()
dimensions <- list()
for (i in 1:no_files){
  dt[[i]]<-fread(files[i], 
                 sep = ",",
                 select = c("nodeNumber","jobId", "taskId", "attemptID", "value", "timestamp"), # dimensions, measures and timestamp
                 integer64="numeric")
  # Attribute labels ("d" for dimensions - "m" for measures - "t" for timestamp)
  if(ncol(dt[[i]])>3)
    dimensions[[i]] <- c("nodeNumber","jobId")
  else
    dimensions[[i]] <- c("nodeNumber")
}

## Filter duplicated rows (it happens sometimes)
for(i in 1:no_files)
  dt[[i]] <- unique(dt[[i]])


## Obtaining partitioning tuples: checking existing partitions
partitioning_tuples <- function(dt, dimensions, no_files){
  tuples <- list()
  t <- 1
  for(i in 1:16){ # only Hadoop events in our case (they contain nodeNumber and jobId)
    tuples[[t]] <- list()
    d <- dt[[i]]
    rows <- dt[[i]][, dimensions[[i]],with=FALSE]
    tuples[[t]] <- unique(rows)
    t <- t + 1
  }
  return(tuples)
}
tuples <- partitioning_tuples(dt, dimensions, no_files)
tuples <- rbindlist(tuples, fill=TRUE)
tuples <- unique(tuples)

## Obtaining whole partitions (all event types) from partitioning tuples
whole_partitions <- function(tuples, dt, attr_labels, files){
  w <- 1
  wP <- list()
  for(i in 1:nrow(tuples)){
    t <- tuples[i]
    wP[[w]] <- list()
    for(j in 1:length(files)){
      d <- dt[[j]]
      if(j == 17 || j == 18 || j == 19){# special case nodeNumber=999, they are not included for the moment
        #wP[[w]][[files[j]]] <- d
        next
      }
      part_attr <- dimensions[[j]]
      for(attr in part_attr)
        d <- d[eval(parse(text=attr)) == t[[attr]] ] 
      if(nrow(d) != 0){
        wP[[w]][[files[j]]] <- d
      }
    }
    w <- w+1
  }
  return(wP)
}


## Obtaining and writing to file whole partitions (all event types) from partitioning tuples
whole_partitions_to_file <- function(tuples, dt, attr_labels, files, path){
  w <- 1
  for(i in 1:nrow(tuples)){
    #cat(paste0("Iteration no: ",i, "\n"),file="log", append=TRUE)
    t <- tuples[i]
    dir = paste0(path,w)
    dir.create(dir)
    for(j in 1:length(files)){
      cat(paste0(j," "),file="log", append=TRUE)
      d <- dt[[j]]
      if(j == 17 || j == 18 || j == 19){# special case nodeNumber=999, they will be included in any partition
        next
      }
      part_attr <- colnames(dt[[j]])[attr_labels[[j]] == "d"]
      for(attr in part_attr)
        d <- d[eval(parse(text=attr)) == t[[attr]] ]
      if(nrow(d) != 0){
        out = paste0(dir,"/part_1_",files[j])
        fwrite(d, out, col.names=TRUE, sep=",")
      }
    }
    cat("\n",file="log", append=TRUE)
    w <- w+1
  }
}

## Find the time window relative to a partition
find_limit <- function(P){
  min = Inf
  max = 0
  for(p in P){
    if(max(p$timestamp) > max)
      max = max(p$timestamp)
    if(min(p$timestamp) < min)
      min = min(p$timestamp)
  }
  return(c(min,max))
}

# Find the time window relative to a given job in a partition
find_limit_job <- function(P){
  min = Inf
  max = 0
  for(i in 1:length(P)){
    p <- P[[i]]
    name <- paste0(names(P)[i],".csv")
    if(names(P)[i]=="Hadoop_positive" | 
       names(P)[i]=="Hadoop_negative" | 
       match(name,files) < 17){ #only look job-specific events
      if(max(p$timestamp) > max)
        max = max(p$timestamp)
      if(min(p$timestamp) < min)
        min = min(p$timestamp)
    }
  }
  return(c(min,max))
}

## Create directory and plots for each partition
w <- sample(1:length(wP),1)
for(w in 1:1){
  P <- wP[[w]]
  par(mfrow=c(1,1))
  xlim <- find_limit_job(P)
  i=1
  #dir = paste0("partitions/partition_",w)
  #dir.create(dir)
  for(p in P){
    x <- p[timestamp >= xlim[1] & timestamp <= xlim[2]]$timestamp
    y <- p[timestamp >= xlim[1] & timestamp <= xlim[2]]$value
    type = tools::file_path_sans_ext(names(P)[i])
    if(!is.null(p$taskId[1]))
      no_tasks <- length(unique(p$taskId))
    else
      no_tasks <- 0
    #jpeg(paste0(dir,"/",type,".jpg"))
    plot(y=y, x=x, xlim=xlim,
         main=paste0(type," nodeNumber: ", p$nodeNumber[1], " jobId: ", p$jobId[1]),
         sub=paste0(" No.points: ",length(y)," No.tasks: ", no_tasks),
         type="p", xlab="timestamp", ylab = "value")
    #dev.off()
    i <- i+1
  }
}

## Read a sample of partitions and plot
for(w in sample(1:nrow(tuples),10)){
  dir = paste0("plots/partition_",w)
  dir.create(dir)
  f = Sys.glob(paste0("partitions/partition_",w,"/*.csv")) # get .csv files from partition w
  no_f <- length(f) 
  P <- list() # set of event types
  for(i in 1:no_f){ # build partition reading files
    name = tools::file_path_sans_ext(f[[i]])
    name <- basename(name)
    P[[name]] = fread(f[i], 
                      sep = ",",
                      select = c("nodeNumber","jobId", "taskId", "attemptID", "value", "timestamp"),
                      integer64="numeric")
  }
  # xlim = find_limit(P)
  # xlim_job = find_limit_job(P)
  i <- 1
  for(p in P){
    x <- p$timestamp
    y <- p$value
    type = tools::file_path_sans_ext(names(P)[i])
    type <- basename(type)
    if(!is.null(p$taskId[1]))
      no_tasks <- length(unique(p$taskId))
    else
      no_tasks <- 0
    jpeg(paste0(dir,"/",type,".jpg"))
    plot(y=y, x=x, #xlim=xlim,
         main=paste0(type," nodeNumber: ", p$nodeNumber[1], " jobId: ", p$jobId[1]),
         sub=paste0(" No.points: ",length(y)," No.tasks: ", no_tasks),
         type="p", xlab="timestamp", ylab = "value")
    dev.off()
    i <- i+1
  }
}