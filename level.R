## Bottom-up
level_segmentation_bottom_up <- function(x, nquant, alpha){
  s <- list()
  i <- 1
  while(i<=(length(x)/2-1)){ # start with log(n) segments
    s[[i]] <- list()
    a = 2*i - 1
    b = 2*i 
    s[[i]][["index"]] <- c(a,b)
    i <- i+1
  }
  # take into account when length is not divisible by 2
  s[[i]] <- list()
  a = 2*i - 1
  b = length(x)
  s[[i]][["index"]] <- c(a,b)
  flag = 0
  
  if(length(nquant)>1)
    probs <- nquant
  else{
    probs = vector(mode="numeric",length=nquant)
    offset <- 0
    for(i in 1:nquant){ # computing the probabilities
      probs[i] <- 1/nquant + offset
      offset <- probs[i]
    }
  }
  q <- unique(quantile(x, probs=probs))
  
  while(flag == 0){ # loop on segments until no merges are possible
    flag = 1
    i <- 1
    min = Inf
    while(i < length(s)){ # try to merge pairs of neighbor segments
      a = s[[i]][["index"]][1]
      b = s[[i+1]][["index"]][2]
      error <- level(x[a:b], q)[["error"]] # get the error (i.e. the minimum one deriving from putting all the data into one segment)
      if(error <= alpha && error < min){ # update best merging pair
        flag = 0
        min <- error
        index <- i
      }
      i<- i+1
    }
    if(flag==0){ # apply the merging
      a <- s[[index]][["index"]][1]
      b <- s[[index+1]][["index"]][2]
      s[[index]][["index"]][2] <- b
      s[[index+1]] <- NULL
    }
  }
  i<-1
  while(i <= length(s)){
    a = s[[i]][["index"]][1]
    b = s[[i]][["index"]][2]
    s[[i]][["level"]] <- level(x[a:b], q)
    i<-i+1
  }
  return(s)
}

## Sliding window
level_segmentation_growing <- function(x, nquant, alpha){
  
  if(length(nquant)>1) # for SWAB algorithm
    probs <- nquant
  else{
    probs = vector(mode="numeric",length=nquant)
    offset <- 0
    for(i in 1:nquant){ # computing the probabilities
      probs[i] <- 1/nquant + offset
      offset <- probs[i]
    }
  }
  q <- unique(quantile(x, probs=probs))
  
  s <- list()
  i = 1
  k = 1
  
  while(i < length(x)){
    j <- i
    repeat{
      j <- j+1
      if(j <= length(x)){
        l <- level(x[i:j],q)
        error <- l$error
      }
      else{
        error <- NULL
        l <- temp
      }
      if(j>length(x) || error > alpha){
        s[[k]] <- list()
        s[[k]]["index"][1] <- i
        s[[k]][["index"]][2] <- j-1
        s[[k]][["level"]] <- l
        i <- j
        k <- k+1
        break
      }
      temp <- l
    }
  }
  i<-1
  while(i <= length(s)){
    a = s[[i]][["index"]][1]
    b = s[[i]][["index"]][2]
    s[[i]][["level"]] <- level(x[a:b], q)
    i<-i+1
  }
  return(s)
}

## Sliding window with max size
level_segmentation_growing_max <- function(x, nquant, alpha, M){
  
  if(length(nquant)>1) 
    probs <- nquant
  else{
    probs = vector(mode="numeric",length=nquant)
    offset <- 0
    for(i in 1:nquant){ # computing the probabilities
      probs[i] <- 1/nquant + offset
      offset <- probs[i]
    }
  }
  q <- unique(quantile(x, probs=probs))
  
  s <- list()
  i = 1
  k = 1
  
  while(i < length(x)){
    j <- i
    repeat{
      j <- j+1
      if(j <= length(x)){
        l <- level(x[i:j],q)
        error <- l$error
      }
      else{
        error <- NULL
        l <- temp
      }
      if(j>length(x) || (j-i)>M || error > alpha){
        s[[k]] <- list()
        s[[k]]["index"][1] <- i
        s[[k]][["index"]][2] <- j-1
        s[[k]][["level"]] <- l
        i <- j
        k <- k+1
        break
      }
      temp <- l
    }
  }
  i<-1
  while(i <= length(s)){
    a = s[[i]][["index"]][1]
    b = s[[i]][["index"]][2]
    s[[i]][["level"]] <- level(x[a:b], q)
    i<-i+1
  }
  return(s)
}

## Level error function
level <- function(x, q){
  min <- Inf
  left <- -Inf
  right <- q[1]
  y <- cut(x, 
           c(-Inf,q), 
           include.lowest=TRUE)
  y <- as.numeric(y)
  for(i in 1:length(q)){ # checking the level with the lowest error
    error <- 0
    for(j in 1:length(x)){ # computing the length for level "i"
      if(x[j] <= left || x[j] > right)
        error <- error + abs(y[j]-i)
    }
    left <- q[i] # updating left and right extreme
    if(i == length(q))
      right <- max(x)
    else
      right <- q[i+1]
    if(error < min){
      min <- error
      level <- i
    }
  }
  if(min == 0)
    error <- 0
  else
    error <- min/length(x)
  return(list("error"= error, "level" = level))
}

## Merging adjacent segments
merge_levels <- function(x, segments, nquant, alpha){
  if(length(nquant)>1) # for computing quantiles
    probs <- nquant
  else{
    probs = vector(mode="numeric",length=nquant)
    offset <- 0
    for(i in 1:nquant){ # computing the probabilities
      probs[i] <- 1/nquant + offset
      offset <- probs[i]
    }
  }
  q <- unique(quantile(x, probs=probs))
  
  flag = 0
  while(flag == 0){
    flag = 1
    i <- 1
    while(i < length(segments)){
      a <- segments[[i]][["level"]]$level
      b <- segments[[i+1]][["level"]]$level
      start <- segments[[i]][["index"]][1]
      end <- segments[[i+1]][["index"]][2]
      if(alpha==0 & a==b){ # alpha is zero: only equal levels are merged
        flag <- 0
        segments[[i]][["index"]][2] <- end
        segments[[i+1]] <- NULL
      }
      else
        if(alpha!=0 & level(x[start:end], q)$error<=alpha){ # check on total error of merging if alpha!=0
          flag <- 0
          segments[[i]][["index"]][2] <- end
          segments[[i+1]] <- NULL
        }
      i <- i+1
    }
  }
  return(segments)
}

## Plotting a level-based segmentation
level_plot <- function(y, segments, nquant){
  #plot(y, main=paste0("Number of quantiles: ", nquant, " Number of segments: ", length(segments)))
  plot(y, type="l")
  for(seg in segments){
    a = seg[["index"]][1]
    b = seg[["index"]][2]
    abline(v=c(a,b), col=seg[["level"]]$level)
    lines(y=y[a:b],x=a:b, col=seg[["level"]]$level, lwd=2)
  }
  if(length(nquant)<=1){
    probs = vector(mode="numeric",length=nquant)
    offset <- 0
    for(i in 1:nquant){ # computing the probabilities
      probs[i] <- 1/nquant + offset
      offset <- probs[i]
    }
  }
  else{
    probs = nquant
    nquant = length(nquant)
  }
  q <- quantile(y, probs=probs)
  abline(h=q, col=c(1:nquant), lwd=2)
}

## Test with level-based segmentation algorithms
nquant <- 3
p <- unlist(result[[20]]$samples)
s1 <- level_segmentation_bottom_up(p, nquant, 0.05)
s2 <- level_segmentation_growing(p, nquant, 0.05)
for(s in list(s1,s2)){
  x <- p
  plot(x, main=paste0("Number of quantiles: ", nquant, " Number of segments: ", length(s)))
  for(seg in s){
    a = seg[["index"]][1]
    b = seg[["index"]][2]
    abline(v=c(a,b), col=seg[["level"]]$level)
    lines(y=x[a:b],x=a:b, col=seg[["level"]]$level, lwd=2)
  }
  probs = vector(mode="numeric",length=nquant)
  offset <- 0
  for(i in 1:nquant){ # computing the probabilities
    probs[i] <- 1/nquant + offset
    offset <- probs[i]
  }
  q <- quantile(x, probs=probs)
  abline(h=q, col=c(1:nquant), lwd=2)
}
# merging
for(s in list(s1,s2)){
  s_bis <- merge_levels(x, s, q, alpha)
  plot(x)
  for(seg in s_bis){
    a = seg[["index"]][1]
    b = seg[["index"]][2]
    abline(v=c(a,b), col=seg[["level"]]$level)
    lines(y=x[a:b],x=a:b, col=seg[["level"]]$level, lwd=2)
  }
}