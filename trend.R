library(Kendall)
library(trend)
library(boot)
library(DAAG)

## Sliding window algorithms and variations (max size and final merging)
build_trends_window <- function(p, w, alpha){
  trends <- list()
  i = 1
  k = 1
  #pdf(paste0("trends_win_",w,".pdf"))
  while(i <= (length(p)-w+1)){
    j <- i+w-1
    test <- MannKendall(p[i:j]) # test on w points (minimum sample)
    if(test$sl<=alpha){ #trend exists: try to grow the segment
      repeat{
        j <- j+1
        test <- MannKendall(p[i:j])  
        if(test$sl>alpha || j >= length(p)+1){ #test is not accepted anymore, break the segment
          j <- j-1
          trends[[k]] <- list()
          trends[[k]][["index"]] <- c(i,j)
          test <- MannKendall(p[i:j])
          trends[[k]][["test"]] <- test
          k <- k+1
          i <- j # start next window after this segments
          break
        }
      }
    }
    i <- i+1
  }
  return(trends)
}
build_trends_window_max <- function(p, w, M, alpha){
  trends <- list()
  i = 1
  k = 1
  while(i <= (length(p)-w+1)){
    j <- i+w-1
    test <- MannKendall(p[i:j]) # test on w points (minimum sample)
    if(test$sl<=alpha){ #trend exists: try to grow the segment
      repeat{
        j <- j+1
        test <- MannKendall(p[i:j])  
        if(test$sl>alpha || j >= length(p) || (j-i)>M){ #test is not accepted anymore, break the segment
          j <- j-1
          trends[[k]] <- list()
          trends[[k]][["index"]] <- c(i,j)
          test <- MannKendall(p[i:j])
          trends[[k]][["test"]] <- test
          k <- k+1
          i <- j # start next window after this segments
          break
        }
      }
    }
    i <- i+1
  }
  return(trends)
}
build_trends_window_max_merge <- function(p, w, M, alpha){
  trends <- list()
  i = 1
  k = 1
  while(i <= (length(p)-w+1)){
    j <- i+w-1
    test <- MannKendall(p[i:j]) # test on w points (minimum sample)
    if(test$sl<=alpha){ #trend exists: try to grow the segment
      repeat{
        j <- j+1
        test <- MannKendall(p[i:j])  
        if(test$sl>alpha || j >= length(p) || (j-i)>M){ #test is not accepted anymore, break the segment
          j <- j-1
          trends[[k]] <- list()
          trends[[k]][["index"]] <- c(i,j)
          test <- MannKendall(p[i:j])
          trends[[k]][["test"]] <- test
          k <- k+1
          i <- j # start next window after this segments
          break
        }
      }
    }
    i <- i+1
  }
  # if(j!=length(p)){ # take into account the last points
  #   test <- MannKendall
  # }
  flag = 0
  while(flag==0){# until no more merges are possible
    i <- 1
    flag = 1
    while(i<length(trends)){ # try to merge neighbor segments if compatible
      a <- trends[[i]][["index"]][1]
      b <- trends[[i]][["index"]][2]
      c <- trends[[i+1]][["index"]][1]
      d <- trends[[i+1]][["index"]][2]
      #if(mk.test(p[a:b],alternative="greater")$p.value <= alpha)
      if(trends[[i]][["test"]]$S > 0)
        main = "INCREASING"
      else
        main = "DECREASING"
      if(b+1!=c){
        i<-i+1
        next
      }
      #if(mk.test(p[c:d],alternative="greater")$p.value <= alpha)
      if(trends[[i+1]][["test"]]$S > 0)
        two = "INCREASING"
      else
        two = "DECREASING"
      test <- MannKendall(p[a:d])
      if(main==two && test$sl<=alpha){
        flag=0
        trends[[i]][["index"]][2] <- d
        trends[[i+1]] <- NULL
        trends[[i]][["test"]] <- test
      }
      i<-i+1
    }
  }
  return(trends)
}

## Bottom-up algorithms and variations (best merge and max size)
build_trends_bottom_up <- function(p, w, alpha){
  trends <- list()
  i <- 1
  while(i<=(length(p)/w-1)){ # start with n/w segments
    trends[[i]] <- list()
    a = w*i - (w-1)
    b = w*i 
    trends[[i]][["index"]] <- c(a,b)
    trends[[i]][["test"]] <- MannKendall(p[a:b])
    i <- i+1
  }
  
  # take into account when length is not divisible by w
  trends[[i]] <- list()
  a = w*i - (w-1)
  b = length(p)
  trends[[i]][["index"]] <- c(a,b)
  trends[[i]][["test"]] <- MannKendall(p[a:b])
  flag = 0
  
  n <- i # no. of segments
  while(flag == 0){ # loop on segments until no merges are possible
    flag = 1
    i <- 1
    while(i < length(trends)){ # try to merge pairs of neighbor segments
      a = trends[[i]][["index"]][1]
      b = trends[[i+1]][["index"]][2]
      test <- MannKendall(p[a:b])
      if(test$sl<=alpha){
        flag = 0
        trends[[i]][["index"]][2] = b
        trends[[i]][["test"]] = test
        trends[[i+1]] <- NULL
        i <- i+1
      }
      else
        i <- i+2
    }
  }
  i<-1
  while(i <= length(trends)){
    a <- trends[[i]][["index"]][1]
    b <- trends[[i]][["index"]][2]
    if(trends[[i]][["test"]]$sl <= alpha){
    }
    else
      trends[[i]] <- NULL
    i<-i+1
  }
  return(trends)
}
build_trends_bottom_up_opt <- function(p, w, alpha){
  trends <- list()
  i <- 1
  while(i<=(length(p)/w-1)){ # start with n/w segments
    trends[[i]] <- list()
    a = w*i - (w-1)
    b = w*i 
    trends[[i]][["index"]] <- c(a,b)
    trends[[i]][["test"]] <- MannKendall(p[a:b])
    i <- i+1
  }
  
  # take into account when length is not divisible by w
  trends[[i]] <- list()
  a = w*i - (w-1)
  b = length(p)
  trends[[i]][["index"]] <- c(a,b)
  trends[[i]][["test"]] <- MannKendall(p[a:b])
  flag = 0
  
  n <- i # no. of segments
  while(flag == 0){ # loop on segments until no merges are possible
    flag = 1
    i <- 1
    min = Inf
    while(i < length(trends)){ # try to merge pairs of neighbor segments
      a = trends[[i]][["index"]][1]
      b = trends[[i+1]][["index"]][2]
      test <- MannKendall(p[a:b])
      if(test$sl<=alpha && test$sl<min){ # update best merging pair
        flag = 0
        min <- test$sl
        index <- i
      }
      i<- i+1
    }
    if(flag==0){ # apply the merging
      a <- trends[[index]][["index"]][1]
      b <- trends[[index+1]][["index"]][2]
      trends[[index]][["index"]][2] <- b
      trends[[index]][["test"]] = MannKendall(p[a:b])
      trends[[index+1]] <- NULL
    }
  }
  i<-1
  while(i <= length(trends)){
    a <- trends[[i]][["index"]][1]
    b <- trends[[i]][["index"]][2]
    if(trends[[i]][["test"]]$sl <= alpha){
    }
    else
      trends[[i]] <- NULL
    i<-i+1
  }
  return(trends)
}
build_trends_bottom_up_opt_max <- function(p, w, M, alpha){
  trends <- list()
  i <- 1
  while(i<=(length(p)/w-1)){ # start with n/w segments
    trends[[i]] <- list()
    a = w*i - (w-1)
    b = w*i 
    trends[[i]][["index"]] <- c(a,b)
    trends[[i]][["test"]] <- MannKendall(p[a:b])
    i <- i+1
  }
  
  # take into account when length is not divisible by w
  trends[[i]] <- list()
  a = w*i - (w-1)
  b = length(p)
  trends[[i]][["index"]] <- c(a,b)
  trends[[i]][["test"]] <- MannKendall(p[a:b])
  flag = 0
  
  n <- i # no. of segments
  while(flag == 0){ # loop on segments until no merges are possible
    flag = 1
    i <- 1
    min = Inf
    while(i < length(trends)){ # try to merge pairs of neighbor segments
      a = trends[[i]][["index"]][1]
      b = trends[[i+1]][["index"]][2]
      if((b-a)<=M){ # max size
        test <- MannKendall(p[a:b])
        sl <- test$sl
      }
      else
        sl <- 1
      if(sl<=alpha && sl<min){ # update best merging pair
        flag = 0
        min <- sl
        index <- i
      }
      i<- i+1
    }
    if(flag==0){ # apply the merging
      a <- trends[[index]][["index"]][1]
      b <- trends[[index+1]][["index"]][2]
      trends[[index]][["index"]][2] <- b
      trends[[index]][["test"]] = MannKendall(p[a:b])
      trends[[index+1]] <- NULL
    }
  }
  i<-1
  while(i <= length(trends)){
    a <- trends[[i]][["index"]][1]
    b <- trends[[i]][["index"]][2]
    if(trends[[i]][["test"]]$sl <= alpha){
    }
    else
      trends[[i]]<-NULL
    i<-i+1
  }
  return(trends)
}

## SWAB with variation (final merging)
build_trends_SWAB <- function(p, w, M, alpha){
  trends <- list()
  i = 1
  k = 1
  offset = 0
  while(i <= (length(p)-M+1)){
    j <- i+M-1
    t <- build_trends_bottom_up_opt(p[i:j],w, alpha) # apply bottom-up in the window
    if(length(t)==0){
      i <- i+1
    }
    else{
      a <- t[[1]][["index"]][1] + offset # take into account offset and update
      b <- t[[1]][["index"]][2] + offset
      t[[1]][["index"]][1] <- a 
      t[[1]][["index"]][2] <- b
      offset <- b
      trends[[k]] <- t[[1]] # take the leftmost segment
      k <- k+1
      i <- offset+1
    }
  }
  while(i <= length(p)-w){ # if the segmentation was not completely applied in the last window
    t <- build_trends_bottom_up_opt(p[i:length(p)],w, alpha)
    if(length(t)>0){
      a <- t[[1]][["index"]][1] + offset # take into account offset and update
      b <- t[[1]][["index"]][2] + offset
      t[[1]][["index"]][1] <- a 
      t[[1]][["index"]][2] <- b
      trends[[k]] <- t[[1]] # take the leftmost segment
      offset <- b
      i <- b+1
      k <- k+1
    }
    else
      break
  }
  return(trends)
}
build_trends_SWAB_merge <- function(p, w, M, alpha){
  trends <- list()
  i = 1
  k = 1
  offset = 0
  while(i <= (length(p)-M+1)){
    j <- i+M-1
    t <- build_trends_bottom_up_opt(p[i:j],w, alpha) # apply bottom-up in the window
    if(length(t)==0){ #if no trend is retrieved
      i <- i+1
    }
    else{
      a <- t[[1]][["index"]][1] + offset # take into account offset and update
      b <- t[[1]][["index"]][2] + offset
      t[[1]][["index"]][1] <- a 
      t[[1]][["index"]][2] <- b
      offset <- b
      trends[[k]] <- t[[1]] # take the leftmost segment
      k <- k+1
      i <- offset+1
    }
  }
  while(i <= length(p)-w){ # if the segmentation was not completely applied in the last window
    t <- build_trends_bottom_up_opt(p[i:length(p)],w, alpha)
    if(length(t)>0){
      a <- t[[1]][["index"]][1] + offset # take into account offset and update
      b <- t[[1]][["index"]][2] + offset
      t[[1]][["index"]][1] <- a 
      t[[1]][["index"]][2] <- b
      trends[[k]] <- t[[1]] # take the leftmost segment
      offset <- b
      i <- b+1
      k <- k+1
    }
    else
      break
  }
  flag = 0
  while(flag==0){ # until no more merges are possible
    i <- 1
    flag = 1
    while(i<length(trends)){ # try to merge neighbor segments if compatible
      a <- trends[[i]][["index"]][1]
      b <- trends[[i]][["index"]][2]
      c <- trends[[i+1]][["index"]][1]
      d <- trends[[i+1]][["index"]][2]
      #if(mk.test(p[a:b],alternative="greater")$p.value <= alpha)
      if(trends[[i]][["test"]]$S >0)
        main = "INCREASING"
      else
        main = "DECREASING"
      if(b+1!=c){
        i<-i+1
        next
      }
      #if(mk.test(p[c:d],alternative="greater")$p.value <= alpha)
      if(trends[[i+1]][["test"]]$S >0)
        two = "INCREASING"
      else
        two = "DECREASING"
      test <- MannKendall(p[a:d])
      if(main==two && test$sl<=alpha){
        flag=0
        trends[[i]][["index"]][2] <- d
        trends[[i+1]] <- NULL
        trends[[i]][["test"]] <- test
      }
      i<-i+1
    }
  }
  return(trends)
}

## Functions to:
## (1) merge adjacent trends
## (2) plot a trend-segmented time series with (3) Kendall-Theil-Sen robust line
## and (4) plot all the trend segments for a time series
merge_trends <- function(p,trends, alpha){
  flag = 0
  while(flag==0){# until no more merges are possible
    i <- 1
    flag = 1
    while(i<length(trends)){ # try to merge neighbor segments if compatible
      a <- trends[[i]][["index"]][1]
      b <- trends[[i]][["index"]][2]
      c <- trends[[i+1]][["index"]][1]
      d <- trends[[i+1]][["index"]][2]
      #if(mk.test(p[a:b],alternative="greater")$p.value <= alpha)
      if(trends[[i]][["test"]]$S >0)
        main = "INCREASING"
      else
        main = "DECREASING"
      if(b+1!=c){
        i<-i+1
        next
      }
      #if(mk.test(p[c:d],alternative="greater")$p.value <= alpha)
      if(trends[[i+1]][["test"]]$S >0)
        two = "INCREASING"
      else
        two = "DECREASING"
      test <- MannKendall(p[a:d])
      if(main==two && test$sl<= alpha){
        flag = 0
        trends[[i]][["index"]][2] <- d
        trends[[i+1]] <- NULL
        trends[[i]][["test"]] <- test
      }
      i<-i+1
    }
  }
  return(trends)
}
plot_trends <- function(p, trends, method, alpha){
  plot(p,type="o",ylab="value",xlab="time",main=paste0(method," ",alpha),sub="RED = increasing , BLUE = decreasing")
  d <- 1
  for(t in trends){
    a <- t[["index"]][1]
    b <- t[["index"]][2]
    if(a!=d){
      lines(y=p[d:(a-1)],x=c(d:(a-1)), type="l", lwd = 4, col="black")
      abline(v=d,col="black")
      abline(v=(a-1),col="black")
    }
    if(t[["test"]]$S > 0)
      c = 2
    else
      c = 4
    lines(y=p[a:b],x=a:b,type="l",col=c,lwd=4)
    abline(v=a,col=c)
    abline(v=b,col=c)
    d <- b+1
  }
  if(d!=length(p)){
    lines(y=p[d:length(p)],x=c(d:length(p)), type="l", lwd = 4, col="black")
    abline(v=d,col="black")
    abline(v=length(p),col="black")
  }
}
plot_trends_theil_sen <- function(y, trends, method, alpha, r){
  plot(y,type="o",ylab="value",xlab="time",main=paste0(method," ",alpha),sub="RED = increasing , BLUE = decreasing")
  d <- 1
  for(t in trends){
    a <- t[["index"]][1]
    b <- t[["index"]][2]
    if(a!=d){
      lines(y=p[d:(a-1)],x=c(d:(a-1)), type="l", lwd = 4, col="black")
      abline(v=d,col="black")
      abline(v=(a-1),col="black")
      
      x <- d:(a-1)
      z <- y[d:(a-1)]
      fit <- mblm(formula = z ~ x, repeated = r)
      z <- fit$coefficients[2]*x + fit$coefficients[1]
      lines(x=x,y=z,lwd=4, col="green", type="l", lty=2)
      
    }
    if(mk.test(p[a:b],alternative="greater")$p.value <= alpha)
      c = 2
    else
      c = 4
    lines(y=p[a:b],x=a:b,type="l",col=c,lwd=4)
    abline(v=a,col=c)
    abline(v=b,col=c)
    d <- b+1
    
    x <- a:b
    z <- y[a:b]
    fit <- mblm(formula = z ~ x, repeated = r)
    z <- fit$coefficients[2]*x + fit$coefficients[1]
    lines(x=x,y=z,lwd=4, col="green", type="l", lty=2)
  }
  if(d!=length(p)){
    lines(y=p[d:length(p)],x=c(d:length(p)), type="l", lwd = 4, col="black")
    abline(v=d,col="black")
    abline(v=length(p),col="black")
    
    x <- d:length(p)
    z <- y[d:length(p)]
    fit <- mblm(formula = z ~ x, repeated = r)
    z <- fit$coefficients[2]*x + fit$coefficients[1]
    lines(x=x,y=z,lwd=4, col="green", type="l", lty=2)
  }
}
multiple_plot_trends <- function(trends, method, alpha){
  for(i in 1:length(trends)){
    a <- trends[[i]][["index"]][1]
    b <- trends[[i]][["index"]][2]
    test <- MannKendall(p[a:b])
    if(test$sl <= alpha){
      if(test$S >0)
        main = "INCREASING"
      else
        main = "DECREASING"
      plot(y=p[a:b],x=a:b,ylab="value",xlab="time",type="o",sub=paste0(main, " Alpha: ",alpha), main=paste0(" No.points: ",length(p[a:b])," Method: ",method))
    }
  }
}

## Synthetic generation function
synthetic <- function(N, L, noise, W){
  result <- list()
  i <- 1
  while(i <= N){
    result[[i]] <- list()
    
    # a random number of breakpoints (from 1 to 20)
    rm(.Random.seed)
    n <- sample(1:20,1)
    
    flag = 1
    while(flag == 1){ # ensuring breaks of at least W samples
      flag = 0
      rm(.Random.seed)
      bp <- sample((1:L),n)
      bp <- unique(sort(bp))
      a = 0
      for(k in 1:(length(bp))){
        if((bp[k]-a) < W)
          flag = 1
        a = bp[k]
      }
      if((L-a) < W)
        flag = 1
    }
    n <- length(bp)
    y <- 1:L
    offset = 0
    sign <- vector(length=n+1)
    
    for(j in 1:(n+1)){
      if(j==1){
        start <- 1
      }
      else
        start <- bp[j-1]+1
      
      if(j==(n+1))
        end <- L
      else
        end <- bp[j]
      
      size <- (end-start+1)
      x <- 1:size
      
      rm(.Random.seed)
      s <- sample(c(-1,0,1),1)
      if(s == 1){
        sign[j] <- 1
        c <- "red"
      }
      else
        if(s == -1){
          sign[j] <- -1
          c <- "blue"
        }
      else{
        sign[j] <- 0
        c <- "black"
      }
      
      z <- sign[j]*(x + x*atan(x) + log(x)^3 + sqrt(x))
      
      # rm(.Random.seed)
      # if(sign[j]!=0){
      #   if(noise!=0)
      #     dev <- sqrt(noise*abs((max(z)-min(z)))/2)
      #   else
      #     dev <- 1
      # }
      # else{
      #   if(offset != 0)
      #     offset <- mean(y[(bp[j-1]:start-1)])
      # }
      rm(.Random.seed)
      #z <- z + rnorm((size),0,dev) + rep(offset,(size))
      y[start:end] <- z + rep(offset,(size))
      offset <- y[end]
    }
    stddev <- noise*sd(y)
    y <- y + rnorm(L,0,stddev)
    #plot(y,type="l")
    result[[i]][["samples"]] <- y
    result[[i]][["bp"]] <- bp
    result[[i]][["sign"]] <- sign
    i <- i+1
  }
  return(result)
}
## Plot the synthetic function with highlighted true trends
plot_synthetic <- function(r){
  s <- r$samples
  y <- unlist(s)
  bp <- r$bp
  length <- length(y)
  n <- length(bp)
  plot(x=1:length, type="n", ylim=c(min(y),max(y)))
  sign <- r$sign
  for(j in 1:(n+1)){
    
    if(j==1){
      start <- 1
    }
    else
      start <- bp[j-1]+1
    
    if(j==(n+1))
      end <- length
    else
      end <- bp[j]
    
    s <- sign[j]
    if(s == 1)
      c = "red"
    else if(s==-1)
      c = "blue"
    else c="black"
    
    lines(x=start:end,y[start:end],type="o",col=c, lwd=2)
    abline(v=bp)
  }
}
## "Edit distance"-based error computation for trend-segmentation
error_synthetic <- function(r, trends){
  s <- r$samples
  y <- unlist(s)
  bp <- r$bp
  length <- length(y)
  n <- length(bp)
  sign <- r$sign
  signs_one <- vector(mode="numeric",length=length)
  for(j in 1:(n+1)){
    
    if(j==1){
      start <- 1
    }
    else
      start <- bp[j-1]+1
    
    if(j==(n+1))
      end <- length
    else
      end <- bp[j]
    
    s <- sign[j]
    signs_one[start:end] <- s
  }
  signs_two <- vector(mode="numeric", length=length)
  for(t in trends){
    if(t[["test"]]$S > 0)
      s <- 1
    else
      s <- -1
    signs_two[t$index[1]:t$index[2]] <- s
  }
  s <- signs_two - signs_one
  return(length(s[abs(s) == 1]))
}

## Synthetic dataset evaluation performances (for each noise, for each alpha, for each w, for each M)
stats <- list()
# computing errors for all the runs
for(noise in c(0.01,0.05,0.1)){
  s <- as.character(noise)
  stats[[s]] <- list()
  for(alpha in c(0.05,0.02,0.01)){
    a <- as.character(alpha)
    stats[[s]][[a]] <- data.table(matrix(rep(0,9*31),nrow = 9, ncol = 31))
    names <- vector(mode="character",length=31)
    i <- 1
    for(w in c(7,10,20,50,100)){
      for(M in c(0,1,2,3,4,5)){
        names[i] <- paste0("W=",w," M=",M)
        i <- i+1
      }
    }
    names[i] <- "avg_time"
    colnames(stats[[s]][[a]]) <- names
    rownames(stats[[s]][[a]]) <- c("window", "bottom-up","bottom-up-opt","bottom-up-opt-max","win-max","win-max-merge","bottom_up_opt_max_merge","swab","swab-merge")
  }
  
  result <- synthetic(20, 2000, noise, 10)
  #pdf(paste0(noise,"_synthetic.pdf"))
  
  for(r in result){ # looping on synthetic signals
    #plot_synthetic(r)
    p <- r$samples
    
    for(alpha in c(0.05,0.02,0.01)){ # looping on alpha
      a <- as.character(alpha)
      for(w in c(7,10,20,50,100)){ # looping on min windows
        M = 0
        time <- system.time(trends <- build_trends_window(p, w, alpha))
        error <- error_synthetic(r,trends)
        column <- paste0("W=",w," M=",M)
        row = 1
        stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
        stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
        # if(length(trends)>0)
        #   plot_trends(p, trends,paste0("window w=",w), alpha)
        
        time <- system.time(trends <- build_trends_bottom_up(p, w, alpha))
        error <- error_synthetic(r,trends)
        column <- paste0("W=",w," M=",M)
        row = 2
        stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
        stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
        # if(length(trends)>0)
        #   plot_trends(p, trends,paste0("bottom_up w=",w), alpha)
        
        time <- system.time(trends <- build_trends_bottom_up_opt(p, w, alpha))
        error <- error_synthetic(r,trends)
        column <- paste0("W=",w," M=",M)
        row = 3
        stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
        stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
        # if(length(trends)>0)
        #   plot_trends(p, trends,paste0("bottom_up_opt w=",w), alpha)
        
        for(M in c(1,2,3,4,5)){
          time <- system.time(trends <- build_trends_bottom_up_opt_max(p, w, M*w, alpha))
          error <- error_synthetic(r,trends)
          column <- paste0("W=",w," M=",M)
          row = 4
          stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
          stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
          # if(length(trends)>0)
          #   plot_trends(p, trends,paste0("bottom_up_opt_max w=",w," M=",M), alpha)
          
          time <- system.time(trends <- build_trends_window_max(p,w,M*w, alpha))
          error <- error_synthetic(r,trends)
          column <- paste0("W=",w," M=",M)
          row = 5
          stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
          stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
          # if(length(trends)>0)
          #   plot_trends(p, trends,paste0("window_max w=",w," M=",M), alpha)
          
          time <- system.time(trends <- build_trends_window_max_merge(p,w,M*w, alpha))
          error <- error_synthetic(r,trends)
          column <- paste0("W=",w," M=",M)
          row = 6
          stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
          stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
          # if(length(trends)>0)
          #   plot_trends(p, trends,paste0("window_max_merge w=",w," M=",M), alpha)
          
          
          time <- system.time({trends <- build_trends_bottom_up_opt_max(p, w, M*w, alpha);
          trends <- merge_trends(p, trends, alpha)})
          error <- error_synthetic(r,trends)
          column <- paste0("W=",w," M=",M)
          row = 7
          stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
          stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
          # if(length(trends)>0)
          #   plot_trends(p, trends,paste0("bottom_up_opt_max_merge w=",w," M=",M), alpha)
          
          time <- system.time(trends <- build_trends_SWAB(p,w,M*w, alpha))
          error <- error_synthetic(r,trends)
          column <- paste0("W=",w," M=",M)
          row = 8
          stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
          stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
          # if(length(trends)>0)
          #   plot_trends(p, trends,paste0("SWAB w=",w," M=",M*w), alpha)
          
          time <- system.time(trends <- build_trends_SWAB_merge(p,w,M*w, alpha))
          error <- error_synthetic(r,trends)
          column <- paste0("W=",w," M=",M)
          row = 9
          stats[[s]][[a]][row,column] <- stats[[s]][[a]][row,column,with=FALSE] + error
          stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31] + time
          # if(length(trends)>0)
          #   plot_trends(p, trends,paste0("SWAB_merge w=",w," M=",M*w), alpha)
          
        }
      }
    }
  }
  # dev.off()
}

# computing total error and average time/error
for(noise in c(0.01,0.05,0.1)){
  s <- as.character(noise)
  for(alpha in c(0.05,0.02,0.01)){
    a <- as.character(alpha)
    stats[[s]][[a]] <- cbind(stats[[s]][[a]], rep(0,9))
    stats[[s]][[a]] <- cbind(stats[[s]][[a]], rep(0,9))
    colnames(stats[[s]][[a]])[32] <- "total_error"
    colnames(stats[[s]][[a]])[33] <- "avg_error"
    rownames(stats[[s]][[a]]) <- c("window", "bottom-up","bottom-up-opt","bottom-up-opt-max","win-max","win-max-merge","bottom_up_opt_max_merge","swab","swab-merge")
    for(row in 1:9){ # computing avg_time, avg_error, total_error
      table <- data.frame(stats[[s]][[a]][row])
      table <- table[1:30]
      numbers <- table[table!=0]
      total_error <- sum(numbers)
      avg_error <- total_error/length(numbers)
      stats[[s]][[a]][row,31] <- stats[[s]][[a]][row,31]/length(numbers)
      stats[[s]][[a]][row,32] <- total_error
      stats[[s]][[a]][row,33] <- avg_error
    }
    table <- data.frame(stats[[s]][[a]])
    m <- head(sort(table[,33]),3)
    first <- rownames(stats[[s]][[a]])[which(table[,33] %in% m[1])]
    second <- rownames(stats[[s]][[a]])[which(table[,33] %in% m[2])]
    third <- rownames(stats[[s]][[a]])[which(table[,33] %in% m[3])]
    print(paste0("Noise= ",noise," alpha=",alpha," FIRST=",first, " Avg_Error=",m[1], " Total_error=",table[which(table[,33] %in% m[1]),32]))
    print(paste0("Noise= ",noise," alpha=",alpha," SECOND=",second, " Avg_Error=",m[2]," Total_error=",table[which(table[,33] %in% m[2]),32]))
    print(paste0("Noise= ",noise," alpha=",alpha," THIRD=",third, " Avg_Error=",m[3]," Total_error=",table[which(table[,33] %in% m[3]),32]))
    print("")
    #write.table(stats[[s]][[a]],file=paste0("noise:",noise,"_alpha:",alpha,".csv"),dec=",",sep=";",col.names=NA)
  }
}
# avg_total_error for each method
total <- rep(0,9)
for(noise in c(0.01,0.05,0.1)){
  s <- as.character(noise)
  for(alpha in c(0.05,0.02,0.01)){
    a <- as.character(alpha)
    total <- total + stats[[s]][[a]][,33]
  }
}
# computing precision 
# 9 is 3(no.noise)*3(no.alpha)
total <- 1-(total/9/20)/2000

# avg_total_time
total <- rep(0,9)
for(noise in c(0.01,0.05,0.1)){
  s <- as.character(noise)
  for(alpha in c(0.05,0.02,0.01)){
    a <- as.character(alpha)
    total <- total + stats[[s]][[a]][,31]
  }
}
total <- total/9/20

# test with random walk
pdf("random.pdf")
for(i in 1:20){
  y <- cumsum(sample(c(-1, 1), 500, TRUE))
  w <- 10
  alpha <- 0.01
  M <- 20
  trends <- build_trends_window_max_merge(y,w,M, alpha)
  plot_trends(y, trends, paste0("window_max_merge min=",w," max=",M), alpha)
}
dev.off()