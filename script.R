source(input.R)
source(trend.R)
source(level.R)

############### Preprocessing for temporal pattern mining -----------------------------
library(Kendall)
library(jmotif)
library(changepoint)

## Reading a partition and converting it into the table format for pattern mining (using SAX, but it is easily to adapt)
## It returns the table, the representations for each event type, the start-end time limits for the job and the cache updated
convert_partition__sax_mining <- function(index, cache){
  f = Sys.glob(paste0("/local/partitions/partition_",index,"/*.csv")) # get .csv files from partition 
  no_f <- length(f) 
  P <- list() # set of event types
  for(i in 1:no_f){ # build partition reading files
    #i = 1
    name = tools::file_path_sans_ext(f[[i]])
    name <- basename(name)
    name <- tail(strsplit(name,paste0("part_",index,"_"))[[1]],1)
    print(paste0("Reading: ", name))
    P[[name]] = fread(f[i], 
                      sep = ",",
                      select = c("nodeNumber","jobId", "taskId", "attemptID", "value", "timestamp"),
                      integer64="numeric")
    if(name=="HadoopDataActivity"){ # converting Hadoop Data Activity into positive and negative values
      p <- P[[name]]
      p <- list("positive" = p[p$value >= 0], "negative" = p[p$value < 0])
      P[[name]] <- NULL
      if(nrow(p$positive) > 0){
        P[[length(P)+1]] <- p$positive
        names(P)[[length(P)]] <- "Hadoop_positive"
      }
      if(nrow(p$negative) > 0){
        P[[length(P)+1]] <- p$negative
        names(P)[[length(P)]] <- "Hadoop_negative"
      }
    }
  }
  # adjusting timestamps and getting time limits relative to the job
  result <- time_adjust_partition(P)
  P_adj <- time_adjust_partition(P)$events
  start <- result$start
  end <- result$end  
  input_table <- list()
  representation <- list()
  k <- 1
  for(i in 1:length(P_adj)){ # building the table for pattern mining
    name <- names(P_adj)[[i]]
    ganglia_name <- paste0(name,".csv")
    cache_name <- paste0(name,":",P_adj[[i]]$nodeNumber[1])
    input_table[[i]] <- list()
    representation[[k]] <- list()
    print(paste0("Converting: ",name))
    if(name== "ReducePeriod" |
       name== "Hadoop_positive" | 
       name == "Hadoop_negative" |
       name == "PullPeriod" |
       name == "MapPeriod"){ # segmentation on continuous Hadoop events
      value <- P_adj[[i]]$value
      time <- P_adj[[i]]$timestamp
      #intervals <- time_series_to_intervals(value, time, "level",list(3,0), name)$intervals
      if(length(value)==1)
        n_paa = 1
      else
        n_paa <- as.integer(length(value)/log2(length(value)))
      result <- time_series_to_intervals(value, time, "SAX",list(n_paa,3), name)
      intervals <- result$intervals
      representation[[k]] <- result$representation
      k <- k+1
    }
    else
      if(match(ganglia_name, files)>=17){ # check if ganglia event is already present in the cache otherwise add to it
        if(!is.null(cache[[cache_name]])){ # the "key" is "type:nodeNumber"
          intervals <- cache[[cache_name]]$intervals
          representation[[k]] <- cache[[cache_name]]$representation
          k <- k+1
        }
        else{
          value <- P_adj[[i]]$value
          time <- P_adj[[i]]$timestamp
          #intervals <- time_series_to_intervals(value, time, "level",list(3,0), name)$intervals
          n_paa <- as.integer(length(value)/log2(length(value)))
          result <- time_series_to_intervals(value, time, "SAX",list(n_paa,3), name)
          intervals <- result$intervals
          representation[[k]] <- result$representation
          # updating the cache
          cache[[cache_name]] <- list()
          cache[[cache_name]]$intervals <- intervals
          cache[[cache_name]]$representation <- result$representation
          k <- k+1
        }
      }
    else{
      s <- P_adj[[i]]
      intervals <- list()
      intervals$start <- s$timestamp
      intervals$end <- s$timestamp
      l <- length(s$timestamp)
      intervals$symbol <- rep(name, l)
    }
    input_table[[i]] <- intervals
  }
  input_table  <- rbindlist(input_table)
  setorder(input_table, start, end, symbol)
  return(list(table = input_table, start = start, end = end, representations = representation, cache = cache))
}

# Writing partition tables to files
no_tuples <- nrow(tuples)
cache <- list()
patterns <- list()
dir.create("/local/converted_partition/")
for(i in 1:no_tuples){
  dir.create(paste0("/local/converted_partition/partition_",i))
  text = paste0("Processing partition: ",i)
  write(x=text,file="log.txt",append=TRUE)
  result <- convert_partition_mining(i, cache)
  j_start <- result$start
  j_end <- result$end
  cache <- result$cache
  input <- result$table
  #input <- input[start >= j_start &  end <= j_end]
  write(x="Partition converted. Writing starts:", file="log.txt", append=TRUE)
  file = paste0("/local/converted_partition/partition_",i,"/table_",1,".csv")
  fwrite(x=input, file=file, sep=";")
  write(x="Partition written.", file="log.txt", append=TRUE)
  #   write(x="Partition converted. Pattern mining starts:", file="log.txt", append=TRUE)
  # window = length(input$start)/length(unique(input$start)) # average number of events per timestamp
  # patterns <- build_tp_patterns_window(input, 10, patterns)
  # write(x="Pattern mining ended.", file="log.txt", append=TRUE)
}

# testing the avg number of events starting in a window of size 5
sum = 0
i = 1
for(s in unique(input$start)){
  sum <- sum + nrow(input[start >= s & start <= (s+10)])
  i <- i +1
}
sum/i

## Test with Ganglia events
table <- list()
i <- 1
for(c in cache){
  table[[i]] <- c$intervals
  table[[i]] <- merge_intervals(table[[i]])
  i <- i+1
}
table <- rbindlist(table)
setorder(table,start,end,symbol)


## Test with Hadoop partition
patterns <- list()
registerDoParallel(cores=10)
stopImplicitCluster()
system.time(patterns <- build_tp_patterns_step_par(input_table[1:1000], 2, patterns))
system.time(patterns <- build_tp_patterns_step(input_table[1:1000], 2, patterns))
system.time(patterns <- build_tp_patterns_window(table[1:100], 2, patterns))
patterns <- build_base_tp_patterns_step(input, 0, patterns)

for(i in 1:length(patterns[[1]])){
  p <- patterns[[1]][[i]]
  print(paste0(names(patterns[[1]])[i]," frequency: ",p$frequency))
}

for(i in 1:length(patterns[[2]])){
  p <- patterns[[2]][[i]]
  print(paste0(names(patterns[[2]])[i]," frequency: ",p$frequency))
}

for(i in 1:length(patterns[[3]])){
  p <- patterns[[3]][[i]]
  rel <- p$relations
  if(p$frequency>1){
    print(rel[!is.na(rel)])
    print(paste0("frequency: ",p$frequency))
    for(j in 1:1){
      table <- rbindlist(p$instances[[j]][[1]])
      plot(xlim=c(min(table$start)-1,max(table$end)+1), main=paste0("frequency: ",p$frequency), type="n", ylim=c(1,1000*(nrow(table)+1)),0,0)
      for(k in 1:nrow(table)){
        t <- table[k]
        segments(x0=t$start, x1=t$end, y0 = k*1000, lwd=5, pch=5, col="red")
        text(x=(t$start+t$end)/2, y=k*1000+200, substr(t$symbol, 1,100), col="blue")
      }
    }
    #dev.off()
  }
}

for(i in 1:length(patterns[[4]])){
  p <- patterns[[4]][[i]]
  rel <- p$relations
  names <- rbindlist(p$instances[[1]][[1]])$symbol
  if(p$frequency>500){
    print(rel[!is.na(rel)])
    print(paste0("frequency: ",p$frequency))
    for(j in 1:1){
      table <- rbindlist(p$instances[[j]][[1]])
      plot(xlim=c(min(table$start),max(table$end)), main=paste0("frequency: ",p$frequency), type="n", ylim=c(1,1000*(nrow(table)+1)),0,0)
      for(k in 1:nrow(table)){
        t <- table[k]
        segments(x0=t$start, x1=t$end, y0 = k*1000, lwd=5, pch=5, col="red")
        text(x=(t$start+t$end)/2, y=k*1000+200, substr(t$symbol, 1,100), col="blue")
      }
    }
    #dev.off()
  }
}



## Plotting SAX with 2-3-4-5 cuts
plot_sax <- function(y, n_paa, ncuts){
  y <- znorm(y)
  paa_y <- paa(y ,n_paa)
  plot(y, type="l")
  sax <- series_to_chars(paa_y, ncuts)
  for(i in 1:(n_paa)){
    a = length(y)/n_paa*(i-1)+1
    b = length(y)/n_paa*i
    segments(x0=a,x1=b,y0 = paa_y[i] ,col="red",pch = 15, lwd = 6)
    text( x=(a+b)/2, y=paa_y[i], sax[i], col="blue",cex=2)
  }
  cuts <- alphabet_to_cuts(ncuts)[2:ncuts]
  abline(h = cuts, lty=2, lwd=2, col="magenta")
  # text(0.5,cuts[1]-1,"a",cex=2,col="magenta")
  # text(0.5, (cuts[1]+cuts[2])/2,"b",cex=2,col="magenta")
  # text(0.5, (cuts[2]+cuts[3])/2,"c",cex=2,col="magenta")
  # text(0.5, (cuts[3]+cuts[4])/2,"d",cex=2,col="magenta")
  # text(0.5, cuts[4]+1,"e",cex=2,col="magenta")
}

## Merge intervals
merge_intervals <- function(intervals){
  flag = 0
  indexes = vector()
  while(flag == 0){
    flag = 1
    i <- 1
    while(i < nrow(intervals)){
      a <- intervals[i]$start
      b <- intervals[i]$end
      c <- intervals[i+1]$start
      d <- intervals[i+1]$end
      s1 <- intervals[i]$symbol
      s2 <- intervals[i+1]$symbol
      if((b+1)==c & s1==s2){
        flag = 0
        intervals[i]$end <- d
        intervals <- intervals[-(i+1)]
      }
      i <- i+1
    }
  }
  return(intervals)
}

## Plotting a (temporal pattern mining) partition
plot_event_partition <- function(table){
  plot(xlim=c(min(table$start),max(table$end)), type="n", ylim=c(1,1000*(nrow(table)+1)),0,0)
  for(i in 1:nrow(table)){
    t <- table[i]
    segments(x0=t$start, x1=t$end, y0 = i*1000, lwd=5, pch=5, col="red")
    text(x=(t$start+t$end)/2, y=i*1000+200, t$symbol, col="blue")
  }
}

## Converting timestamps in a partition to obtain a consistent time window:
## all the existing timestamps in the partition are searched for and timestamps of each event type
## are "indexed" according to this list of instants of time
time_adjust_partition <- function(P){
  i <- 1
  t <- list()
  for(p in P){
    t[[i]] <- p$timestamp
    i <- i+1
  }
  t <- unlist(t)
  t <- unique(t)
  t <- sort(t)
  for(i in 1:length(P)){
    p <- P[[i]]
    p$timestamp <- match(p$timestamp,t)
    P[[i]] <- p
  }
  limits <- find_limit_job(P) # also returning the time boundaries relative to the job of the partition
  return(list(events = P, start = limits[1], end = limits[2]))
}


## Function to transform a time series into an interval using "Trend", "Level" or "SAX" abstraction
## "parameters" is passed to the segmentation method and contains the parameters needed
time_series_to_intervals <- function(data, time, method, parameters, name){
  representation <- list()
  intervals <- list()
  if(method == "trend"){ # Trend Segmentation
    i <- 1
    d <- 1
    trends <- build_trends_window_max_merge(data, parameters[[1]], parameters[[2]], parameters[[3]])
    for(t in trends){
      a <- t[["index"]][1]
      b <- t[["index"]][2]
      if(a != d){
        intervals[[i]] <- list("start" = d, "end" = (a-1), "symbol" = paste0(name,".flat"))
        i <- i+1
      }
      if(t[["test"]]$S > 0)
        symbol = "increasing"
      else
        symbol = "decreasing"
      intervals[[i]] <- list("start" = a, "end" = b, "symbol" = paste0(name,".",symbol))
      d <- b+1
      i <- i+1
    }
    if(d != length(data)){
      intervals[[i]] <- list("start" = d, "end" = length(data), "symbol" = paste0(name,".flat"))
    }
    representation <- trends
  }
  else
    if(method == "level"){ # Level segmentation
      i <- 1
      segments <- level_segmentation_growing_max(data, parameters[[1]],parameters[[2]], 1000)
      segments <- merge_levels(data, segments, parameters[[1]], parameters[[2]])
      #segments <- level_segmentation_growing(data, parameters[[1]], parameters[[2]])
      for(s in segments){
        a <- s[["index"]][1]
        b <- s[["index"]][2]
        intervals[[i]] <- list("start" = a, "end" = b, "symbol" = paste0(name,".level:",s$level$level))
        i <- i + 1
      }
      representation <- segments
    }
  else if(method == "SAX"){
    intervals <- list()
    i <- i
    z <- znorm(data)
    # n_paa <- as.integer(2*log(length(y)))
    n_paa <- parameters[[1]]
    paa_y <- paa(z ,n_paa)
    sax <- series_to_chars(paa_y, parameters[[2]])
    for(i in 1:(n_paa)){
      a = length(z)/n_paa*(i-1)+1
      b = length(z)/n_paa*i
      intervals[[i]] <- list("start" = a, "end" = b, "symbol" = paste0(name,".sax:",sax[i]))
      i <- i + 1
    }
    representation <- sax
  }
  #intervals <- merge_intervals(intervals)
  intervals <- data.table(rbindlist(intervals))
  for(i in 1:nrow(intervals)){
    intervals[i]$start <- time[intervals[i]$start]
    intervals[i]$end <- time[intervals[i]$end]
  }
  return(list(intervals=intervals, representation=representation))
}

############### Sketching the mining algorithm (Top-k temporal patterns) -------------

## Relations are build "left-to-right" (e.g. a.end <= b.start, b.end <= c.start):
## this explains the way conditions are checked
## N.B. for the moment only k=2 relations is allowed
temporal_relation <- function(symbols, k){
  r <- matrix(nrow=length(symbols)-1,ncol=length(symbols)-1)
  if(k == 2){
    l <- length(symbols)-1
    for(i in 1:l){
      a <- symbols[[i]]
      for(j in (i+1):(l+1)){
        b <- symbols[[j]]
        if(a$end < b$start)
          r[i,j-1] <- paste0(a$symbol,"_before_",b$symbol)
        else
          r[i,j-1] <- paste0(a$symbol,"_co-occurs_",b$symbol)
      }
    }
  }
  result <- list(relations = r, instances = list(symbols))
  return(result)
}
bi_relation <- function(a, b, k){
  if(k == 2){
    if(a$end < b$start)
      text <- paste0(a$symbol,"_before_",b$symbol)
    else
      text <- paste0(a$symbol,"_co-occurs_",b$symbol)
  }
  return(text)
}


## Structure of a pattern: 
## relation is NULL for 1-length, a string for 2-length
## and a matrix for k-length (symmetric, rows ordered lexicographically by start, end, symbol)
## (they are indexed using the string of relations)


## Building base 1-2-3-temporal patterns 
build_base_tp_patterns_step <- function(partition, step, patterns){
  if(length(patterns)==0){
    patterns <- list()
    p1 <- list()
    p2 <- list()
    p3 <- list()
  }
  else{ # if patterns are searched in another partition
    p1 <- patterns[[1]]
    p2 <- patterns[[2]]
    p3 <- patterns[[3]]
  }
  for(i in 1:nrow(partition)){ #partition is a data.table (start,end,symbol)
    current <- partition[i]
    symbol <- current$symbol
    start <- current$start
    end <- current$end
    # if(is.null(p1[[symbol]])){
    #   p1[[symbol]]<- list()
    #   p1[[symbol]]$relations <- NULL
    #   p1[[symbol]]$instances <- list()
    #   p1[[symbol]]$instances[[1]] <- current
    #   p1[[symbol]]$frequency <- 1
    # }
    # else
    #   p1[[symbol]]$frequency <- p1[[symbol]]$frequency + 1
    for(j in (i+1):(i+step)){ # looping on all the other intervals and building all the candidate 2 patterns including this symbol
      if(j > nrow(partition) | j < (i+1))
        break
      candidate <- partition[j]
      r <- bi_relation(current, candidate, 2)
      # checking if pattern already exists
      if(is.null(p2[[r]])){
        p2[[r]] <- list()
        p2[[r]]$relations <- r
        p2[[r]]$instances <- list()
        p2[[r]]$instances[[1]] <- list(current, candidate)
        p2[[r]]$frequency <- 1
      }
      else{
        p2[[r]]$frequency <- p2[[r]]$frequency + 1
        p2[[r]]$instances[[length(p2[[r]]$instances)+1]] <- list(current, candidate)
      }
      for(k in (j+1):(j+step)){ # extending to 3-pattern
        if(k > nrow(partition) | k < (j+1))
          break
        candidate_bis <- partition[k]
        # computing the relation of order 3 within this symbol and the previous pattern
        symbols <- list(current, candidate, candidate_bis)
        r <- temporal_relation(symbols, 2)
        text <- r$relations
        index <- paste(text, collapse="-")
        if(is.null(p3[[index]])){ #checking if pattern already exists
          # index <- search_pattern(p3, r)
          # if(index == 0){
          # index <- length(p3)+1
          p3[[index]] <- list()
          p3[[index]]$relations <- r$relations
          p3[[index]]$frequency <- 1
          p3[[index]]$instances <- list()
          p3[[index]]$instances[[1]] <- symbols
        }
        else{
          p3[[index]]$frequency <- p3[[index]]$frequency + 1
          p3[[index]]$instances[[length(p3[[index]]$instances)+1]] <- list(current, candidate, candidate_bis)
        }
      }
    }
  }
  patterns[[1]] <- p1
  patterns[[2]] <- p2
  patterns[[3]] <- p3
  return(patterns)
}
build_base_tp_patterns_window <- function(partition, window, patterns){
  if(length(patterns)==0){
    patterns <- list()
    p1 <- list()
    p2 <- list()
    p3 <- list()
  }
  else{ # if patterns are searched in another partition
    p1 <- patterns[[1]]
    p2 <- patterns[[2]]
    p3 <- patterns[[3]]
  }
  for(i in 1:nrow(partition)){ #partition is a data.table (start,end,symbol)
    current <- partition[i]
    symbol <- current$symbol
    start <- current$start
    end <- current$end
    # if(is.null(p1[[symbol]])){
    #   p1[[symbol]]<- list()
    #   p1[[symbol]]$relations <- NULL
    #   p1[[symbol]]$instances <- list()
    #   p1[[symbol]]$instances[[1]] <- current
    #   p1[[symbol]]$frequency <- 1
    # }
    # else
    #   p1[[symbol]]$frequency <- p1[[symbol]]$frequency + 1
    for(j in (i+1):nrow(partition)){ # looping on all the other intervals and building all the candidate 2 patterns including this symbol
      candidate <- partition[j]
      if(j>nrow(partition) || candidate$end > (end+window))
        break
      r <- bi_relation(current, candidate, 2)
      # checking if pattern already exists
      if(is.null(p2[[r]])){
        p2[[r]] <- list()
        p2[[r]]$relations <- r
        p2[[r]]$instances <- list()
        p2[[r]]$instances[[1]] <- list(current, candidate)
        p2[[r]]$frequency <- 1
      }
      else{
        p2[[r]]$frequency <- p2[[r]]$frequency + 1
        p2[[r]]$instances[[length(p2[[r]]$instances)+1]] <- list(current, candidate)
      }
      for(k in (j+1):nrow(partition)){ # extending to 3-pattern
        candidate_bis <- partition[k]
        if(k>nrow(partition) || candidate_bis$end > (end+window))
          break
        # computing the relation of order 3 within this symbol and the previous pattern
        symbols <- list(current, candidate, candidate_bis)
        r <- temporal_relation(symbols, 2)
        text <- r$relations
        index <- paste(text, collapse="-")
        if(is.null(p3[[index]])){ #checking if pattern already exists
          # index <- search_pattern(p3, r)
          # if(index == 0){
          # index <- length(p3)+1
          p3[[index]] <- list()
          p3[[index]]$relations <- r$relations
          p3[[index]]$frequency <- 1
          p3[[index]]$instances <- list()
          p3[[index]]$instances[[1]] <- symbols
        }
        else{
          p3[[index]]$frequency <- p3[[index]]$frequency + 1
          p3[[index]]$instances[[length(p3[[index]]$instances)+1]] <- r$instances
        }
      }
    }
  }
  patterns[[1]] <- p1
  patterns[[2]] <- p2
  patterns[[3]] <- p3
  return(patterns)
}

## Building all temporal patterns
build_tp_patterns_window <- function(partition, window, patterns){
  if(length(patterns)==0){
    patterns <- list()
    patterns[[1]] <- list()
  }
  for(i in 1:nrow(partition)){ #partition is a data.table (start,end,symbol)
    current <- partition[i]
    symbol <- current$symbol
    start <- current$start
    end <- current$end
    # if(is.null(patterns[[1]][[symbol]])){
    #   patterns[[1]][[symbol]]<- list()
    #   patterns[[1]][[symbol]]$relations <- NULL
    #   patterns[[1]][[symbol]]$instances <- list()
    #   patterns[[1]][[symbol]]$instances[[1]] <- list(current)
    #   patterns[[1]][[symbol]]$frequency <- 1
    # }
    # else{
    #   patterns[[1]][[symbol]]$frequency <- patterns[[1]][[symbol]]$frequency + 1
    #   patterns[[1]][[symbol]]$instances[[length(patterns[[1]][[symbol]]$instances)+1]] <- symbol
    # }
    ## build all the possible left-to-right patterns containing "current" in a right-window of size "window"
    patterns_to_add <- build_all_relations_window(list(current), partition, i+1, end+window)
    for(p in patterns_to_add){
      n <- length(p$instances[[1]])
      text <- p$relations
      index <- paste(text, collapse="-")
      if(length(patterns) < n)
        patterns[[n]] <- list()
      if(is.null(patterns[[n]][[index]])){
        patterns[[n]][[index]] <- list()
        patterns[[n]][[index]]$relations <- p$relations
        patterns[[n]][[index]]$frequency <- 1
        patterns[[n]][[index]]$instances <- list()
        patterns[[n]][[index]]$instances[[1]] <- p$instances
      }
      else{
        patterns[[n]][[index]]$frequency <- patterns[[n]][[index]]$frequency + 1
        patterns[[n]][[index]]$instances[[length(patterns[[n]][[index]]$instances)+1]] <- p$instances
      }
    }
  }
  return(patterns)
}
build_tp_patterns_step <- function(partition, step, patterns){
  if(length(patterns)==0){
    patterns <- list()
    patterns[[1]] <- list()
  }
  for(i in 1:nrow(partition)){ #partition is a data.table (start,end,symbol)
    current <- partition[i]
    symbol <- current$symbol
    start <- current$start
    end <- current$end
    # if(is.null(patterns[[1]][[symbol]])){
    #   patterns[[1]][[symbol]]<- list()
    #   patterns[[1]][[symbol]]$relations <- NULL
    #   patterns[[1]][[symbol]]$instances <- list()
    #   patterns[[1]][[symbol]]$instances[[1]] <- list(current)
    #   patterns[[1]][[symbol]]$frequency <- 1
    # }
    # else{
    #   patterns[[1]][[symbol]]$frequency <- patterns[[1]][[symbol]]$frequency + 1
    #   patterns[[1]][[symbol]]$instances[[length(patterns[[1]][[symbol]]$instances)+1]] <- symbol
    # }
    ## build all the possible left-to-right patterns containing "current" in a right-window of size "step">=1
    patterns_to_add <- build_all_relations_step(list(current), partition[(i+1):(i+step)])
    for(p in patterns_to_add){
      n <- length(p$instances[[1]])
      text <- p$relations
      index <- paste(text, collapse="-")
      if(length(patterns) < n)
        patterns[[n]] <- list()
      if(is.null(patterns[[n]][[index]])){
        patterns[[n]][[index]] <- list()
        patterns[[n]][[index]]$relations <- p$relations
        patterns[[n]][[index]]$frequency <- 1
        patterns[[n]][[index]]$instances <- list()
        patterns[[n]][[index]]$instances[[1]] <- p$instances
      }
      else{
        patterns[[n]][[index]]$frequency <- patterns[[n]][[index]]$frequency + 1
        patterns[[n]][[index]]$instances[[length(patterns[[n]][[index]]$instances)+1]] <- p$instances
      }
    }
  }
  return(patterns)
}

## Recursive functions to build all relations
build_all_relations_window <- function(current, partition, index, limit){
  patterns <- list()
  if(length(current)!=1) # first recursion level: a symbol calls himself
    patterns[[1]] <- temporal_relation(current, 2)
  for(j in index:nrow(partition)){ # gathering all the candidates
    candidate <- partition[j]
    if(j>nrow(partition) || candidate$end > limit){
      flag = 0
      break
    }
    temp <- current
    temp[[length(temp)+1]] <- candidate
    patterns_to_add <- build_all_relations_window(temp, partition, j+1, limit)
    for(p in patterns_to_add){
      patterns[[length(patterns)+1]] <- p
    }
  }
  return(patterns)
}
build_all_relations_step <- function(current, partition, step){
  patterns <- list()
  if(length(current)!=1) # first recursion level: a symbol calls himself
    patterns[[1]] <- temporal_relation(current, 2)
  for(j in 1:nrow(partition)){ # gathering all the candidates
    candidate <- partition[j]
    if(is.na(candidate$start)){
      break
    }
    temp <- current
    temp[[length(temp)+1]] <- candidate
    patterns_to_add <- build_all_relations_step(temp, partition[j+1:nrow(partition)])
    for(p in patterns_to_add){
      patterns[[length(patterns)+1]] <- p
    }
  }
  return(patterns)
}


## Testing forEach
## Building all temporal patterns
library(doParallel)
registerDoParallel(cores=2)
stopImplicitCluster()

build_tp_patterns_window_par <- function(partition, window, patterns){
  if(length(patterns)==0){
    patterns <- list()
  }
  patterns_to_add <- list()
  result <- foreach(i=1:nrow(partition), .verbose=TRUE) %dopar% {
    current <- partition[i]
    symbol <- current$symbol
    start <- current$start
    end <- current$end
    # we don't care about the 1-lenght patterns
    build_all_relations_window(list(current), partition, i+1, end+window)
  }
  patterns_to_add <- unlist(result, recursive=FALSE)
  print("Fine costruzione.")
  for(i in 1:length(patterns_to_add)){
    p <- patterns_to_add[[i]]
    n <- length(p$instances[[1]])
    text <- p$relations
    index <- paste(text, collapse="-")
    if(length(patterns) < n)
      patterns[[n]] <- list()
    if(is.null(patterns[[n]][[index]])){
      patterns[[n]][[index]] <- list()
      patterns[[n]][[index]]$relations <- p$relations
      patterns[[n]][[index]]$frequency <- 1
      patterns[[n]][[index]]$instances <- list()
      patterns[[n]][[index]]$instances[[1]] <- p$instances
    }
    else{
      patterns[[n]][[index]]$frequency <- patterns[[n]][[index]]$frequency + 1
      patterns[[n]][[index]]$instances[[length(patterns[[n]][[index]]$instances)+1]] <- p$instances
    }
  }
  return(patterns)
}
build_tp_patterns_step_par <- function(partition, step, patterns){
  if(length(patterns)==0){
    patterns <- list()
    patterns[[1]] <- list()
  }
  patterns_to_add <- list()
  result <- foreach(i=1:nrow(partition), .verbose=TRUE) %dopar% {
    current <- partition[i]
    symbol <- current$symbol
    start <- current$start
    end <- current$end
    # we don't care about the 1-lenght patterns
    build_all_relations_step(list(current), partition[(i+1):(i+step)])
  }
  patterns_to_add <- unlist(result, recursive=FALSE)
  print("Fine costruzione.")
  for(i in 1:length(patterns_to_add)){
    p <- patterns_to_add[[i]]
    n <- length(p$instances[[1]])
    text <- p$relations
    index <- paste(text, collapse="-")
    if(length(patterns) < n)
      patterns[[n]] <- list()
    if(is.null(patterns[[n]][[index]])){
      patterns[[n]][[index]] <- list()
      patterns[[n]][[index]]$relations <- p$relations
      patterns[[n]][[index]]$frequency <- 1
      patterns[[n]][[index]]$instances <- list()
      patterns[[n]][[index]]$instances[[1]] <- p$instances
    }
    else{
      patterns[[n]][[index]]$frequency <- patterns[[n]][[index]]$frequency + 1
      patterns[[n]][[index]]$instances[[length(patterns[[n]][[index]]$instances)+1]] <- p$instances
    }
  }
  return(patterns)
}


############### Retail store case --------
## type - customer_id - product_id - price - quantity
## event occurrences are modeled as a poisson distribution with lambda=1/60
no_ids = 5
no_actions = 100 # no. of actions for customer (normal sale, indecision, shoplift)
no_customers = 1 # no. of customers

products <- list()
for(i in 1:no_ids){
  rm(.Random.seed)
  products[[i]] <- list("price"=sample(10:50,1))
}


shoplift <- list() # list of shoplifting timestamps for each customer
lambda = 1/300
stream <- list()
for(c in 1:no_customers){
  t <- 0
  for(i in 1:no_actions){ # randomly generating one action
    l <- length(stream)+1
    rm(.Random.seed)
    id <- sample(1:no_ids,1)
    rm(.Random.seed)
    quantity = sample(1:10,1)
    price = products[[id]]$price
    
    rm(.Random.seed)
    p <- sample(1:100,1)
    if(p < 6){ # shoplifting (shelf - exit)
      rm(.Random.seed)
      t1 <- t + (-log(runif(1))/lambda)
      rm(.Random.seed)
      t2 <- t1 + (-log(runif(1))/lambda)
      
      stream[[l]] <- list("customer"=c,"type"="shelf", "id"=id,
                          "price"= price, 
                          "quantity"=quantity, "timestamp"=t1)
      stream[[l+1]] <- list("customer"=c,"type"="exit", "id"=id,
                            "price"= price, 
                            "quantity"=quantity, "timestamp"=t2)
      t <- t2
      print(paste0("Shoplifting: ",t, " id: ",id))
      shoplift[[length(shoplift)+1]] <- list("customer"=c,"id"=id,"timestamp"=t2)
    }
    else{
      rm(.Random.seed)
      t1 <- t + (-log(runif(1))/lambda)
      
      rm(.Random.seed)
      q <- sample(1:100,1) # return to shelf: shelf-shelf
      if(q < 11){
        print(paste0("return to shelf: ",t2, " id=",id))
        stream[[l]] <- list("customer"=c,"type"="shelf", "id"=id,
                            "price"= price, 
                            "quantity"=quantity, "timestamp"=t1)
        
        rm(.Random.seed)
        t2 <- t1 + (-log(runif(1))/lambda)
        
        stream[[l+1]] <- list("customer"=c,"type"="shelf", "id"=id,
                              "price"= price, 
                              "quantity"=quantity, "timestamp"=t2)
        t <- t2
        next
      }
      
      rm(.Random.seed)
      t2 <- t1 + (-log(runif(1))/lambda)
      rm(.Random.seed)
      t3 <- t2 + (-log(runif(1))/lambda)
      
      
      stream[[l]] <- list("customer"=c,"type"="shelf", "id"=id,
                          "price"= price, 
                          "quantity"=quantity, "timestamp"=t1)
      stream[[l+1]] <- list("customer"=c,"type"="counter", "id"=id,
                            "price"= price, 
                            "quantity"=quantity, "timestamp"=t2)
      stream[[l+2]] <- list("customer"=c,"type"="exit", "id"=id,
                            "price"= price, 
                            "quantity"=quantity, "timestamp"=t3)
      t <- t3
    }
  }
}


## partitioning the stream by event type
shelf <- list()
counter <- list()
exit <- list()
for(s in stream){
  if(s$type == "shelf")
    shelf[[length(shelf)+1]] <- s
  if(s$type == "counter")
    counter[[length(counter)+1]] <- s
  if(s$type == "exit")
    exit[[length(exit)+1]] <- s
}
## partitioning by customer
customer <- list()
for(c in 1:no_customers){
  customer[[c]] <- list()
  for(s in stream){
    if(s$customer==c)
      customer[[c]][[length(customer[[c]])+1]] <- s
  }
}

## building partitions by (customer, product)
list_of_partition <- list()
for(c in 1:no_customers){
  for(id in 1:no_ids){
    l <- length(list_of_partition)
    list_of_partition[[l+1]] <- list()
    for(s in stream){
      if(s$id==id & s$customer==c)
        list_of_partition[[l+1]][[length(list_of_partition[[l+1]])+1]] <- s 
    }
  }
}

# producing partition input_tables for each partition
for(i in 1:length(list_of_partition)){
  part <- list_of_partition[[i]]
  input_table <- list()
  j <- 1
  if(length(part)>0){
    for(s in part){
      input_table[[j]] <- list()
      input_table[[j]]$start <- s$timestamp
      input_table[[j]]$end <- s$timestamp
      input_table[[j]]$symbol <- s$type
      j <- j + 1
    }
    list_of_partition[[i]] <- rbindlist(input_table)
    setorder(list_of_partition[[i]],start,end,symbol)
  }
}

patterns <- list()
patterns <- build_tp_patterns_window(list_of_partition[[1]],2/lambda, patterns)
patterns <- build_base_tp_patterns_window(list_of_partition[[1]], 2/lambda, patterns)

patterns <- list()
partition <- list_of_partition[[1]]
for(partition in list_of_partition){ # scanning partitions
  #candidates <- build_base_tp_patterns_step(partition, 0, patterns) # building candidate patterns from signals in the partition
  #candidates <- build_base_tp_patterns_window(partition, 1.5/lambda, patterns)
  candidates <- build_tp_patterns_window(partition, 1/lambda, patterns)
  #candidates <- build_tp_patterns_step(partition, 2, patterns)
  patterns[[1]] <- candidates[[1]]
  patterns[[2]] <- candidates[[2]]
  patterns[[3]] <- candidates[[3]]
}
# outputting top-10 patterns for 2 lengths

for(i in 1:length(patterns[[2]])){
  p <- patterns[[2]][[i]]
  print(paste0(names(patterns[[2]])[i]," frequency: ",p$frequency))
}

for(i in 1:length(patterns[[3]])){
  p <- patterns[[3]][[i]]
  rel <- p$relations
  print(rel[!is.na(rel)])
  print(paste0("frequency: ",p$frequency))
  # if(nrow(rbindlist(p$instances[[1]])[symbol=="fire.yes"])>1){
  if(p$frequency>30){
    for(j in 1:1){
      table <- rbindlist(p$instances[[j]][[1]])
      plot(xlim=c(min(table$start),max(table$end)), main=paste0("frequency: ",p$frequency), type="n", ylim=c(1,1000*(nrow(table)+1)),0,0)
      for(k in 1:nrow(table)){
        t <- table[k]
        segments(x0=t$start, x1=t$end, y0 = k*1000, lwd=5, pch=5, col="red")
        text(x=(t$start+t$end)/2, y=k*1000+200, substr(t$symbol, 1,100), col="blue")
      }
    }
    #dev.off()
  }
}



############### Stock trend case -----------
no_ids = 10 # no. of stocks

## generating time series and news events
## normal behaviour: up-flat-down-flat or down-flat-up-flat (no abrupt changes)
ts <- list()
news_events <- list()
for(i in 1:no_ids){ # generating stock signals of about 1000 points each
  rm(.Random.seed)
  offset <- 0
  start <- 1
  y <- rep(1,10000)
  for(j in 1:10){
    x1 <- sample(20:40,1)
    x2 <- sample(20:40,1)
    x3 <- sample(20:40,1)
    
    # first trend
    rm(.Random.seed)
    sign <- sample(c(-1,1),1)
    x <- 1:x1
    z <- sign*(x + x*atan(x) + log(x)^3 + sqrt(x))
    y[start:(start+x1-1)] <- z + rep(offset,(x1))
    offset <- y[(start+x1-1)]
    start <- start+x1
    
    rm(.Random.seed)
    p <- sample(1:100,1)
    if(p > 20){ # normality: flat - down/up - flat
      y[start:(start+x2-1)] <- rep(offset,(x2))
      offset <- y[start+x2-1]
      start <- start+x2
      
      z <- -sign*(1:x3 + 1:x3*atan(1:x3) + log(1:x3)^3 + sqrt(1:x3))
      y[start:(start+x3-1)] <- z + rep(offset,(x3))
      offset <- y[start+x3-1]
      start <- start+x3
      
      y[start:(start+x2-1)] <- rep(offset,(x2))
      offset <- y[start+x2-1]
      start <- start+x2
    }
    else{ # big news: up (down) - down (up)
      print(paste0("Abrupt for product: ",i, " at time=",start))
      news_events[[length(news_events)+1]] <- list("type"="news", "value"=0, "id"=i,"timestamp"=start)
      
      z <- -sign*(1:x3 + 1:x3*atan(1:x3) + log(1:x3)^3 + sqrt(1:x3))
      y[start:(start+x3-1)] <- z + rep(offset,(x3))
      offset <- y[start+x3-1]
      start <- start+x3
      
      y[start:(start+x2-1)] <- rep(offset,(x2))
      offset <- y[start+x2-1]
      start <- start+x2
    }
  }
  y <- y[1:start-1]
  stddev <- 0.01*sd(y)
  y <- y + rnorm(length(y),0,stddev)
  ts[[i]] <- y
}

# building the stream of stock event
lambda = 1/60
ts_events <- list()
for(i in 1:length(ts)){
  ts_events[[i]] <- list()
  j <- 1
  t  <- 0
  for(y in ts[[i]]){
    t <- t+(-log(runif(1))/lambda)
    t <- t + 1
    ts_events[[i]][[j]] <- list("type"="stock","value"=y,"id"=i,"timestamp"=t)
    j <- j+1
  }
  ts_events[[i]] <- rbindlist(ts_events[[i]])
}
# updating the news events timestamps accordingly
for(n in 1:length(news_events)){
  news <- news_events[[n]]
  news$timestamp <- ts_events[[news$id]][news$timestamp]$timestamp
  news_events[[n]] <- news
}
ts_events <- rbindlist(ts_events)
news_events <- rbindlist(news_events)
stream <- list(ts_events)
stream <- rbindlist(stream)
setorder(stream, timestamp, type)

## partition by stock
list_of_partition <- list()
for(s in 1:no_ids){
  stock <- stream[id == s & type=="stock"]
  stock <- list("value"=stock$value,"time"=stock$timestamp)
  stock <- time_series_to_intervals(stock$value,stock$time,"trend",c(10,20,0.01), paste0("stock"))
  news <- news_events[id == s]
  news$start <- news$timestamp
  news$end <- news$timestamp
  news$symbol <- news$type
  news$id <- NULL
  news$timestamp <- NULL
  news$type <- NULL
  news$value <- NULL
  input_table <- list(stock$intervals, news)
  input_table  <- rbindlist(input_table)
  setorder(input_table, start, end, symbol)
  list_of_partition[[s]] <- input_table
}

patterns <- list()
for(partition in list_of_partition){ # scanning partitions
  candidates <- build_tp_patterns_window(partition, 20/lambda, patterns) # building candidate patterns from signals in the partition
  #candidates <- build_base_tp_patterns_window(partition, 1.5/lambda, patterns)
  patterns[[1]] <- candidates[[1]]
  patterns[[2]] <- candidates[[2]]
  patterns[[3]] <- candidates[[3]]
}
# outputting top-10 patterns for 2 lengths
for(i in 1:length(patterns[[2]])){
  p <- patterns[[2]][[i]]
  print(paste0(names(patterns[[2]])[i]," frequency: ",p$frequency))
}

for(i in 1:length(patterns[[3]])){
  p <- patterns[[3]][[i]]
  rel <- p$relations
  print(rel[!is.na(rel)])
  print(paste0("frequency: ",p$frequency))
  # if(nrow(rbindlist(p$instances[[1]])[symbol=="fire.yes"])>1){
  if(p$frequency<30){
    for(j in 1:1){
      table <- rbindlist(p$instances[[j]][[1]])
      plot(xlim=c(min(table$start),max(table$end)), main=paste0("frequency: ",p$frequency), type="n", ylim=c(1,1000*(nrow(table)+1)),0,0)
      for(k in 1:nrow(table)){
        t <- table[k]
        segments(x0=t$start, x1=t$end, y0 = k*1000, lwd=5, pch=5, col="red")
        text(x=(t$start+t$end)/2, y=k*1000+200, substr(t$symbol, 1,100), col="blue")
      }
    }
    #dev.off()
  }
}



############### Fire case ------------
## Fire if temp > 50 and humidity > 10
no_events <- 100
lambda <- 1/60
stream <- list()
no_areas = 5

for(a in 1:no_areas){
  t = 0
  for(i in 1:no_events){
    rm(.Random.seed)
    p <- sample(1:100,1)
    l <- length(stream)+1
    t1 <- t -log(runif(1))/lambda
    t2 <- t1 -log(runif(1))/lambda
    t <- t2
    if(p < 10){ # fire
      print(paste0("Fire at: ",t2," in area: ",a))
      stream[[l]] <- list("area"=a, "type"="temperature","value"=sample(51:100,1),"timestamp"=t1)
      stream[[l+1]] <- list("area"=a, "type"="humidity","value"=sample(11:20,1),"timestamp"=t2)
      stream[[l+2]] <- list("area"=a, "type"="fire","value"="yes",timestamp=(t2+t1)/2)
    }
    else{
      stream[[l]] <- list("area"=a, "type"="temperature","value"=sample(1:50,1),"timestamp"=t1)
      stream[[l+1]] <- list("area"=a, "type"="humidity","value"=sample(1:10,1),"timestamp"=t2)
      stream[[l+2]] <- list("area"=a, "type"="fire","value"="no","timestamp"=(t2+t1)/2)
    }
  }
}

# partitioning by type
temperature <- list()
humidity <- list()
for(s in stream){
  if(s$type=="humidity"){
    l <- length(humidity)+1
    humidity[[l]] <- s
  }
  if(s$type=="temperature"){
    l <- length(temperature)+1
    temperature[[l]] <- s
  }
}

# partitioning by area
list_of_partition <- list()
stream <- rbindlist(stream)
for(a in 1:no_areas){
  t <- stream[area == a & type=="temperature"]
  t <- list("value"=as.numeric(t$value),"timestamp"=as.numeric(t$timestamp))
  temp_int <- time_series_to_intervals(t$value, t$timestamp,"level",list(5,0), "temp")
  #level_plot(t$value, temp_int$representation, 5)
  
  h <- stream[area == a & type=="humidity"]
  h <- list("value"=as.numeric(h$value),"timestamp"=as.numeric(h$timestamp))
  hum_int <- time_series_to_intervals(h$value, h$timestamp,"level",list(5,0), "hum")
  #level_plot(h$value, hum_int$representation, 5)
  
  others <- stream[area==a & type == "fire"]
  others$area <- NULL
  others$start <- others$timestamp
  others$end <- others$timestamp
  others$symbol <- paste0(others$type,".",others$value)
  others$timestamp <- NULL
  others$type <- NULL
  others$value <- NULL
  
  table <- list(temp_int$intervals, hum_int$intervals, others)
  table <- rbindlist(table)
  setorder(table,start,end,symbol)
  list_of_partition[[a]] = table
}

patterns <- list()
for(partition in list_of_partition){ # scanning partitions
  candidates <- build_tp_patterns_step(partition, 2, patterns) # building candidate patterns from signals in the partition
  #candidates <- build_base_tp_patterns_window(partition, 1.5/lambda, patterns)
  patterns[[1]] <- candidates[[1]]
  patterns[[2]] <- candidates[[2]]
  patterns[[3]] <- candidates[[3]]
}
# outputting top-10 patterns for 2 lengths
for(i in 1:length(patterns[[2]])){
  p <- patterns[[2]][[i]]
  print(paste0(names(patterns[[2]])[i]," frequency: ",p$frequency))
}

for(i in 1:length(patterns[[3]])){
  p <- patterns[[3]][[i]]
  rel <- p$relations
  print(rel[!is.na(rel)])
  print(paste0("frequency: ",p$frequency))
  # if(nrow(rbindlist(p$instances[[1]])[symbol=="fire.yes"])>1){
  if(p$frequency<10){
    for(j in 1:1){
      table <- rbindlist(p$instances[[j]][[1]])
      plot(xlim=c(min(table$start),max(table$end)), main=paste0("frequency: ",p$frequency), type="n", ylim=c(1,1000*(nrow(table)+1)),0,0)
      for(k in 1:nrow(table)){
        t <- table[k]
        segments(x0=t$start, x1=t$end, y0 = k*1000, lwd=5, pch=5, col="red")
        text(x=(t$start+t$end)/2, y=k*1000+200, substr(t$symbol, 1,100), col="blue")
      }
    }
    #dev.off()
  }
}
